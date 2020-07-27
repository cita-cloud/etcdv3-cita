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

SERVER_ADDRESS = "localhost:12345"


class KVServer(rpc_pb2_grpc.KVServicer):
    def __init__(self, channel):
        super().__init__()
        self.stub = rpc_pb2_grpc.KVStub(channel)

    def Range(self, request, context):
        """Range gets the keys in the range from the key-value store.
        """
        print(f"Request:\n```\n{request}\n```\n")
        resp = self.stub.Range(request)
        print(f"Response:\n```\n{resp}\n```\n")
        return resp

    def Put(self, request, context):
        """Range gets the keys in the range from the key-value store.
        """
        print(f"Request:\n```\n{request}\n```\n")
        resp = self.stub.Put(request)
        print(f"Response:\n```\n{resp}\n```\n")
        return resp


class WatchServer(rpc_pb2_grpc.WatchServicer):
    def __init__(self, channel):
        super().__init__()
        self.stub = rpc_pb2_grpc.WatchStub(channel)

    def Watch(self, request_iterator, context):
        """Watch watches for events happening or that have happened. Both input and output
        are streams; the input stream is for creating and canceling watchers and the output
        stream sends events. One watch RPC can watch on multiple key ranges, streaming events
        for several watches at once. The entire event history can be watched starting from the
        last compaction revision.
        """
        def process(req_it):
            try:
                for req in req_it:
                    print(f"Watch Request:\n```\n{req}\n```\n")
                    yield req
            except Exception as e:
                print(f"Catch exception:\n```\n{e}\n```\n")

        resp_stream = self.stub.Watch(process(request_iterator))
        for resp in resp_stream:
            print(f"Watch Response:\n```\n{resp}\n```\n")
            yield resp


def serve():
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            rpc_pb2_grpc.add_KVServicer_to_server(KVServer(channel), server)
            rpc_pb2_grpc.add_WatchServicer_to_server(WatchServer(channel), server)
            server.add_insecure_port('[::]:2379')
            server.start()
            server.wait_for_termination()
        except Exception as e:
            print(f"Server Catch exception:\n```\n{e}\n```\n")


if __name__ == '__main__':
    print("server start.")
    logging.basicConfig()
    serve()

