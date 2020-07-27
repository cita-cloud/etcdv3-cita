import logging
import grpc
import rpc_pb2
import kv_pb2
import rpc_pb2_grpc
import itertools
from queue import Queue
from threading import Thread, Lock, Condition
from sortedcontainers import SortedDict

from concurrent import futures
from contract_client import ContractClient

# CITA client url and test key.
URL = 'http://127.0.0.1:1337'
PRIVATE_KEY = '0xc2c9d4828cd0755542d0dfc9deaf44f2f40bb13d35f5907a50f60d8ccabf9832'


# ResponseHeader taken from a real etcd response.
HEADER = rpc_pb2.ResponseHeader(
    cluster_id=3104097219258285814,
    member_id=8302883879756480553,
    revision=7,
    raft_term=2
)


# An improvised mpsc channel
class Channel:
    def __init__(self):
        self.chan = []
        self.cv_lock = Lock()
        self.cv = Condition(self.cv_lock)
        self.lock = Lock()
        self.is_closed = False

    def put(self, value):
        with self.lock:
            self.chan.append(value)
            with self.cv_lock:
                self.cv.notify()

    def get(self):
        with self.lock:
            ret = self.chan[:]
            self.chan.clear()
            return ret

    def close(self):
        with self.lock:
            self.is_closed = True
            with self.cv_lock:
                self.cv.notify()

    def is_closed(self):
        with self.lock:
            return self.is_closed

    def wait(self):
        while not self.is_closed:
            with self.cv_lock:
                self.cv.wait()
                chan = self.get()
                for r in chan:
                    yield r


class Backend:
    def __init__(self):
        self.kv = KV()
        self.watchers = {}
        self.keys_followed = {}
        self.revision = 0

    def add_watcher(self, req, chan):
        key = req.key
        range_end = req.range_end
        watch_id = req.watch_id
        watcher = Watcher(req, chan, self)
        self.watchers[watch_id] = watcher
        keys_to_follow = self.kv.irange(key, range_end)
        for key in keys_to_follow:
            if key in self.keys_followed:
                self.keys_followed.append(watcher)
            else:
                self.keys_followed[key] = [watcher]

    def cancel_watcher(self, req):
        watch_id = req.watch_id
        watcher = self.watcher[watch_id]
        key = watcher.key
        range_end = watcher.range_end
        keys_to_cancel = self.kv.irange(key, range_end)
        for key in keys_to_cancel:
            if key in self.keys_followed:
                retained = []
                for w in self.keys_followed[key]:
                    if w.watch_id == watch_id:
                        w.cancel()
                    else:
                        retained.append(w)
                self.keys_followed[key] = retained

    # Keys and values
    def kv_kvrange(self, key, range_end):
        return self.kv.kvrange(key, range_end)

    # Entries
    def kv_erange(self, key, range_end):
        return self.kv.erange(key, range_end)

    # keys
    def kv_irange(self, key, range_end):
        return self.kv.irange(key, range_end)

    def kv_put(self, key, value, lease):
        if key in self.keys_followed:
            watchers = self.keys_followed[key]
            for w in watchers:
                w.notify_put(self.kv_get(key))
        self.revision += 1
        entry = kv_pb2.KeyValue(
            create_revision=self.revision,
            mod_revision=self.revision,
            version=1,
            key=key, value=value, lease=lease,
        )
        self.kv.put(key, entry, lease)

    def kv_del(self):
        if key in self.keys_followed:
            watchers = self.keys_followed[key]
            for w in watchers:
                w.notify_del(self.kv_get(key))
        self.revision += 1
        self.kv.delete(key, key + b'\0')

    def kv_get(self, key):
        return self.kv_kvrange(key, key + b'\0')


class KV:
    def __init__(self, *args, **kwargs):
        self.kv = SortedDict(*args, **kwargs)
        self.client = ContractClient(URL, PRIVATE_KEY)

    # Keys and values
    def kvrange(self, key, range_end):
        entries = self.erange(key, range_end)
        return [(e.key, e.value) for e in entries]

    # Entries
    def erange(self, key, range_end):
        keys = self.irange(key, range_end)
        return [self.kv[k] for k in keys]

    # Keys
    def irange(self, key, range_end):
        assert key != b'', 'Key cannot be empty.'
        left = \
            0 if key == b'\0' else \
            self.kv.bisect_left(key)
        right = \
            len(self.kv) if range_end == b'\0' else \
            left + 1 if range_end == b'' else \
            self.kv.bisect_left(range_end)
        return self.kv.keys()[left: right]

    def delete(self, key, range_end):
        keys = self.irange(key, range_end)
        prev_kvs = [
            kv_pb2.KeyValue(key=k, value=self.kv[k])
            for k in keys
        ]
        for k in keys:
            del self.kv[k]
        return len(keys), prev_kvs

    def put(self, key, entry, lease=0):
        self.client.kv_put(key, entry.value, lease)
        self.kv.update({key: entry})


class Watcher:
    def __init__(self, req, chan, backend):
        self.key = req.key
        self.range_end = req.range_end
        self.watch_id = req.watch_id
        self.prev_kv = req.prev_kv
        self.put_aware = \
            rpc_pb2.WatchCreateRequest.NOPUT not in req.filters
        self.delete_aware = \
            rpc_pb2.WatchCreateRequest.NODELETE not in req.filters
        self.chan = chan
        self.backend = backend

    def cancel(self):
        self.chan.close()

    def notify_put(self, kv, prev_kv=None):
        if self.put_aware:
            event = kv_pb2.Event(
                type=kv_pb2.Event.PUT,
                kv=kv,
                prev_kv=prev_kv,
            )
            resp = rpc_pb2.WatchResponse(
                header=HEADER,
                watch_id=self.watch_id,
                compact_revision=1,
                events=[event],
            )
            self.chan.put(resp)

    def notify_del(self):
        if self.delete_aware:
            resp = rpc_pb2.WatchResponse(
                header=HEADER,
                watch_id=self.watch_id,
                compact_revision=1,
            )
            self.chan.put(resp)


class TestKVServicer(rpc_pb2_grpc.KVServicer):

    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    def Range(self, request, context):
        """Range gets the keys in the range from the key-value store.
        """
        print(f"Range: `{request}`")
        key = request.key
        range_end = request.range_end
        results = self.backend.kv_kvrange(key, range_end)
        if request.limit > 0:
            reuslts = itertools.islice(results, request.limit)
        if request.count_only:
            resp = rpc_pb2.RangeResponse(
                header=HEADER,
                count=sum(1 for _ in results),
            )
        else:
            resp = rpc_pb2.RangeResponse(
                header=HEADER,
                kvs=[kv_pb2.KeyValue(key=k, value=v) for k, v in results],
            )
        print(f"Range resp: `{resp}`")
        return resp

    def Put(self, request, context):
        """Put puts the given key into the key-value store.
        A put request increments the revision of the key-value store
        and generates one event in the event history.
        """
        # context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        # context.set_details('Method not implemented!')
        # raise NotImplementedError('Method not implemented!')
        print(f"Put: `{request}`")
        key = request.key
        value = request.value
        lease = request.lease
        self.backend.kv_put(key, value, lease)
        return rpc_pb2.PutResponse(header=HEADER)

    def DeleteRange(self, request, context):
        """DeleteRange deletes the given range from the key-value store.
        A delete request increments the revision of the key-value store
        and generates a delete event in the event history for every deleted key.
        """
        # context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        # context.set_details('Method not implemented!')
        # raise NotImplementedError('Method not implemented!')
        print(f"DeleteRange: `{request}`")
        key = request.key
        range_end = request.range_end
        cnt, prev_kvs = self.backend.kv_del(key, range_end)
        if request.prev_kvs:
            return rpc_pb2.DeleteRangeResponse(
                header=HEADER,
                deleted=cnt,
                prev_kvs=prev_kvs,
            )
        else:
            return rpc_pb2.DeleteRangeResponse(
                header=HEADER,
                deleted=cnt,
            )

    def Txn(self, request, context):
        """Txn processes multiple requests in a single transaction.
        A txn request increments the revision of the key-value store
        and generates events with the same revision for every completed request.
        It is not allowed to modify the same key several times within one txn.
        """
        print(f"Txn: `{request}`")
        cmps = request.compare
        true_ops = request.success
        false_ops = request.failure

        def apply_compare(cmps):
            for cmp in cmps:
                expected_result = cmp.result
                compare_target = cmp.WhichOneof("target_union")
                target_value = getattr(cmp, compare_target)
                key = cmp.key
                range_end = cmp.range_end
                entries = self.backend.kv_erange(key, range_end)
                values = [getattr(e, compare_target) for e in entries]

                CompareResult = rpc_pb2.Compare.CompareResult
                cmp_ops = {
                    CompareResult.EQUAL: lambda v: v == target_value,
                    CompareResult.NOT_EQUAL: lambda v: v != target_value,
                    CompareResult.LESS: lambda v: v < target_value,
                    CompareResult.GREATER: lambda v: v > target_value,
                }
                return all(map(cmp_ops[expected_result], values))

        def apply_request_ops(request_ops):
            resps = []
            for op in request_ops:
                op_type = op.WhichOneof('request')
                from rpc_pb2 import ResponseOp
                dispatcher = {
                    "request_range":
                        lambda req: ResponseOp(response_range=self.Range(req, context)),
                    "request_put":
                        lambda req: ResponseOp(response_put=self.Put(req, context)),
                    "request_delete_range":
                        lambda req: ResponseOp(response_delete_range=self.DeleteRange(req, context)),
                    "request_txn":
                        lambda req: ResponseOp(response_txn=self.Txn(req, context)),
                }
                op_resp = dispatcher[op_type](getattr(op, op_type))
                resps.append(
                    dispatcher[op_type](getattr(op, op_type))
                )
            return resps

        compare_result = apply_compare(cmps)
        resps = apply_request_ops(
            true_ops if compare_result else
            false_ops
        )
        txn_resp = rpc_pb2.TxnResponse(
            header=HEADER,
            succeeded=compare_result,
            responses=resps,
        )
        print(f'Txn Resp: `{txn_resp}')
        return txn_resp

    def Compact(self, request, context):
        """Compact compacts the event history in the etcd key-value store. The key-value
        store should be periodically compacted or the event history will continue to grow
        indefinitely.
        """
        print(f"Compact: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


class TestLeaseServicer(rpc_pb2_grpc.LeaseServicer):
    """Missing associated documentation comment in .proto file."""

    def LeaseGrant(self, request, context):
        """LeaseGrant creates a lease which expires if the server does not receive a keepAlive
        within a given time to live period. All keys attached to the lease will be expired and
        deleted if the lease expires. Each expired key generates a delete event in the event history.
        """
        print(f"LeaseGrant: `{request}`")
        # ID = THE_CLIENT.lease_grant(request.TTL, request.ID)
        # return rpc_pb2_grpc.LeaseGrantResponse(ID=ID)

        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def LeaseRevoke(self, request, context):
        """LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
        """
        print(f"LeaseRevoke: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def LeaseKeepAlive(self, request_iterator, context):
        """LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
        to the server and streaming keep alive responses from the server to the client.
        """
        print(f"LeaseKeepAlive: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def LeaseTimeToLive(self, request, context):
        """LeaseTimeToLive retrieves lease information.
        """
        print(f"LeaseTimeToLive: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def LeaseLeases(self, request, context):
        """LeaseLeases lists all existing leases.
        """
        print(f"LeaseLeases: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


class TestMaintenanceServicer(rpc_pb2_grpc.MaintenanceServicer):
    """Missing associated documentation comment in .proto file."""

    def Alarm(self, request, context):
        """Alarm activates, deactivates, and queries alarms regarding cluster health.
        """
        print(f"Alarm: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Status(self, request, context):
        """Status gets the status of the member.
        """
        print(f"Status: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Defragment(self, request, context):
        """Defragment defragments a member's backend database to recover storage space.
        """
        print(f"Defragment: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Hash(self, request, context):
        """Hash computes the hash of whole backend keyspace,
        including key, lease, and other buckets in storage.
        This is designed for testing ONLY!
        Do not rely on this in production with ongoing transactions,
        since Hash operation does not hold MVCC locks.
        Use "HashKV" API instead for "key" bucket consistency checks.
        """
        print(f"Hash: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def HashKV(self, request, context):
        """HashKV computes the hash of all MVCC keys up to a given revision.
        It only iterates "key" bucket in backend storage.
        """
        print(f"HashKV: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Snapshot(self, request, context):
        """Snapshot sends a snapshot of the entire backend from a member over a stream to a client.
        """
        print(f"Snapshot: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def MoveLeader(self, request, context):
        """MoveLeader requests current leader node to transfer its leadership to transferee.
        """
        print(f"MoveLeader: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


class TestAuthServicer(rpc_pb2_grpc.AuthServicer):
    """Missing associated documentation comment in .proto file."""

    def AuthEnable(self, request, context):
        """AuthEnable enables authentication.
        """
        print(f"AuthEnable: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AuthDisable(self, request, context):
        """AuthDisable disables authentication.
        """
        print(f"AuthDisable: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Authenticate(self, request, context):
        """Authenticate processes an authenticate request.
        """
        print(f"Authenticate: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserAdd(self, request, context):
        """UserAdd adds a new user. User name cannot be empty.
        """
        print(f"UserAdd: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserGet(self, request, context):
        """UserGet gets detailed user information.
        """
        print(f"UserGet: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserList(self, request, context):
        """UserList gets a list of all users.
        """
        print(f"UserList: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserDelete(self, request, context):
        """UserDelete deletes a specified user.
        """
        print(f"UserDelete: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserChangePassword(self, request, context):
        """UserChangePassword changes the password of a specified user.
        """
        print(f"UserChangePassword: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserGrantRole(self, request, context):
        """UserGrant grants a role to a specified user.
        """
        print(f"UserGrantRole: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UserRevokeRole(self, request, context):
        """UserRevokeRole revokes a role of specified user.
        """
        print(f"UserRevokeRole: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleAdd(self, request, context):
        """RoleAdd adds a new role. Role name cannot be empty.
        """
        print(f"RoleAdd: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleGet(self, request, context):
        """RoleGet gets detailed role information.
        """
        print(f"RoleGet: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleList(self, request, context):
        """RoleList gets lists of all roles.
        """
        print(f"RoleList: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleDelete(self, request, context):
        """RoleDelete deletes a specified role.
        """
        print(f"RoleDelete: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleGrantPermission(self, request, context):
        """RoleGrantPermission grants a permission of a specified key or range to a specified role.
        """
        print(f"RoleGrantPermission: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RoleRevokePermission(self, request, context):
        """RoleRevokePermission revokes a key or range permission of a specified role.
        """
        print(f"RoleRevokePermisssion: `{request}`")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


class TestWatchServicer(rpc_pb2_grpc.WatchServicer):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, backend):
        super().__init__()
        self.backend = backend
        self.watch_id = 0

    def next_id(self):
        self.watch_id += 1
        return self.watch_id

    def Watch(self, request_iterator, context):
        """Watch watches for events happening or that have happened. Both input and output
        are streams; the input stream is for creating and canceling watchers and the output
        stream sends events. One watch RPC can watch on multiple key ranges, streaming events
        for several watches at once. The entire event history can be watched starting from the
        last compaction revision.
        """
        print(f"Watch: `{request_iterator}`")

        chan = Channel()

        resps = map(lambda r: self.HandleWatchRequest(r, chan), request_iterator)
        for resp in resps:
            print(f"Watch req resp: `{resp}`")
            yield resp
        for resp in chan.wait():
            print(f"Watch channel resp: `{resp}`")
            yield resp

    def HandleWatchRequest(self, req, chan):
        print(f"Watch recv: `{req}")
        req_type = req.WhichOneof('request_union')
        dispatcher = {
            "create_request":
                lambda req: self.HandleCreateRequest(req.create_request, chan),
            "cancel_request":
                lambda req: self.HandleCancelRequest(req.cancel_request),
            "progress_request":
                lambda req: self.HandleProgressRequest(req.progress_request),
        }
        return dispatcher[req_type](req)

    def HandleCreateRequest(self, req, chan):
        print("Handle Create Request")
        watch_id = req.watch_id or self.next_id()
        self.backend.add_watcher(req, chan)
        return rpc_pb2.WatchResponse(
            header=HEADER,
            created=True,
        )

    def HandleCancelRequest(self, req):
        print("Handle Cancel Request")
        key = req.key
        range_end = req.range_end
        watch_id = req.watch_id
        self.backend.cancel_watcher(req)
        return rpc_pb2.WatchResponse(
            header=HEADER,
            watch_id=watch_id,
            canceled=True,
            compact_revision=1,
        )

    def HandleProgressRequest(self, req):
        print("Handle progress Request")
        key = req.key
        range_end = req.range_end
        watch_id = req.watch_id
        # TODO implement progress request
        return rpc_pb2.WatchResponse(
            header=HEADER,
            watch_id=watch_id,
            compact_revision=1,
        )


def serve():
    backend = Backend()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc_pb2_grpc.add_KVServicer_to_server(TestKVServicer(backend), server)
    rpc_pb2_grpc.add_LeaseServicer_to_server(TestLeaseServicer(), server)
    rpc_pb2_grpc.add_MaintenanceServicer_to_server(TestMaintenanceServicer(), server)
    rpc_pb2_grpc.add_AuthServicer_to_server(TestAuthServicer(), server)
    rpc_pb2_grpc.add_WatchServicer_to_server(TestWatchServicer(backend), server)
    server.add_insecure_port('[::]:2379')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    print("server start.")
    logging.basicConfig()
    serve()
