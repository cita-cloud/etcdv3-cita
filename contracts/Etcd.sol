pragma solidity 0.4.24;

contract Etcd {
    struct Value {
        bool is_valid;
        // create_revision is the revision of last creation on this key.
        uint64 create_revision;
        // mod_revision is the revision of last modification on this key.
        uint64 mod_revision;
        // version is the version of the key. A deletion resets
        // the version to zero and any modification of the key
        // increases its version.
        uint64 version;
        // value is the value held by the key, in bytes.
        bytes value;
        // lease is the ID of the lease that attached to key.
        // When the attached lease expires, the key will be deleted.
        // If lease is 0, then no lease is attached to the key.
        int64 lease;
    }
    // key => value
    mapping (bytes => Value) _keyValueMap;
    
    struct Lease {
        uint start;
        uint64 ttl;
    }
    // LeaseID => ttl
    mapping (int64 => Lease) _leaseMap;
    int64 _lease_id = 1;
    
    function kv_put(bytes key, bytes value, int64 lease) public {
        uint64 revison = uint64(block.number);
        bool is_valid = _keyValueMap[key].is_valid;
        if (is_valid) {
            uint64 version = _keyValueMap[key].version;
            _keyValueMap[key].mod_revision = revison;
            _keyValueMap[key].version = version + 1;
            _keyValueMap[key].value = value;
            _keyValueMap[key].lease = lease;
        } else {
            _keyValueMap[key] = Value(true, revison, revison, 1, value, lease);
        }
    }
    
    function kv_get(bytes key) view public returns (bytes) {
        if (_keyValueMap[key].is_valid == false) {
            return ;
        }
        int64 lease = _keyValueMap[key].lease;
        if (lease != 0) {
            Lease storage l = _leaseMap[lease];
            if ((l.start + uint(l.ttl * 1000)) <= now) {
                return ;
            }
        }
        return _keyValueMap[key].value;
    }
    
    function kv_del(bytes key) public {
        _keyValueMap[key].is_valid = false;
        _keyValueMap[key].version = 0;
    }

    function kv_is_valid(bytes key) view public returns (bool) {
        return _keyValueMap[key].is_valid;
    }

    function kv_create_revision(bytes key) view public returns (uint64) {
        return _keyValueMap[key].create_revision;
    }

    function kv_mod_revision(bytes key) view public returns (uint64) {
        return _keyValueMap[key].mod_revision;
    }

    function kv_version(bytes key) view public returns (uint64) {
        if (_keyValueMap[key].is_valid) {
            return _keyValueMap[key].version;
        } else {
            return 0;
        }
    }

    function kv_lease(bytes key) view public returns (int64) {
        return _keyValueMap[key].lease;
    }
    
    event LeaseGrant(int64 indexed id);
    
    function lease_grant(int64 lease, uint64 ttl) public {
        if (lease != 0) {
            _leaseMap[lease] = Lease(now, ttl);
        } else {
            emit LeaseGrant(_lease_id);
            _leaseMap[_lease_id] = Lease(now, ttl);
            _lease_id = _lease_id + 1;
        }
    }

    function lease_id() view public returns (int64) {
        return _lease_id - 1;
    }
    
    function lease_revoke(int64 lease) public {
        _leaseMap[lease].ttl = 0;
    }
    
    function lease_keepalive(int64 lease) public {
        _leaseMap[lease].start = now;
    }
    
    function lease_time2live(int64 lease) view public returns (uint64, uint64) {
        uint64 ttl = _leaseMap[lease].ttl;
        uint64 remain = uint64(uint(ttl * 1000) +  _leaseMap[lease].start - now);
        return (ttl, remain);
    }
}
