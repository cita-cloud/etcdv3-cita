#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# pylint: disable=missing-docstring

from cita import CitaClient, ContractClass
from pathlib import Path
import time

class ContractClient():
    # pylint: disable=too-many-instance-attributes

    def __init__(self, url, private_key):
        client = CitaClient(url, timeout=10)
        etcd_class = ContractClass(Path('contracts/Etcd.sol'), client)
        etcd_obj, contract_addr, tx_hash = etcd_class.instantiate(private_key)

        self.url = url
        self.client = client
        self.etcd_obj = etcd_obj
        self.contract_addr = contract_addr

    def kv_put(self, key, value, lease):
        tx_hash = self.etcd_obj.kv_put(key, value, lease)
        self.client.confirm_transaction(tx_hash)

    def kv_get(self, key):
        return self.etcd_obj.kv_get(key)

    def kv_del(self, key):
        tx_hash = self.etcd_obj.kv_del(key)
        self.client.confirm_transaction(tx_hash)

    def kv_is_valid(self, key):
        return self.etcd_obj.kv_is_valid(key)

    def kv_create_revision(self, key):
        return self.etcd_obj.kv_create_revision(key)

    def kv_mod_revision(self, key):
        return self.etcd_obj.kv_mod_revision(key)

    def kv_version(self, key):
        return self.etcd_obj.kv_version(key)

    def kv_lease(self, key):
        return self.etcd_obj.kv_lease(key)

    def lease_grant(self, lease, ttl):
        tx_hash = self.etcd_obj.lease_grant(lease, ttl)
        self.client.confirm_transaction(tx_hash)
        if lease == 0:
            id = self.etcd_obj.lease_id()
        else:
            id = lease
        return id

    def lease_revoke(self, lease):
        tx_hash = self.etcd_obj.lease_revoke(lease)
        self.client.confirm_transaction(tx_hash)

    def lease_keepalive(self, lease):
        tx_hash = self.etcd_obj.lease_keepalive(lease)
        self.client.confirm_transaction(tx_hash)

    def lease_time2live(self, lease):
        ttl, remain = self.etcd_obj.lease_time2live()
        return (ttl, remain)

if __name__ == '__main__':
    c = ContractClient('http://127.0.0.1:1337', '0xc2c9d4828cd0755542d0dfc9deaf44f2f40bb13d35f5907a50f60d8ccabf9832')
    # kv test
    print('put key: /test/a')
    c.kv_put(b'/test/a', b'1', 0)

    # print info of key
    print('get key /test/a : ', c.kv_get(b'/test/a'))
    print('is valid of /test/a : ', c.kv_is_valid(b'/test/a'))
    print('create_revision of /test/a : ', c.kv_create_revision(b'/test/a'))
    print('mod_revision of /test/a : ', c.kv_mod_revision(b'/test/a'))
    print('version of /test/a : ', c.kv_version(b'/test/a'))
    print('lease of /test/a : ', c.kv_lease(b'/test/a'))

    print('modify key: /test/a')
    c.kv_put(b'/test/a', b'2', 0)

    # print info of key
    print('get key /test/a : ', c.kv_get(b'/test/a'))
    print('is valid of /test/a : ', c.kv_is_valid(b'/test/a'))
    print('create_revision of /test/a : ', c.kv_create_revision(b'/test/a'))
    print('mod_revision of /test/a : ', c.kv_mod_revision(b'/test/a'))
    print('version of /test/a : ', c.kv_version(b'/test/a'))
    print('lease of /test/a : ', c.kv_lease(b'/test/a'))

    print('del key: /test/a')
    c.kv_del(b'/test/a')

    # print info of key
    print('get key /test/a : ', c.kv_get(b'/test/a'))
    print('is valid of /test/a : ', c.kv_is_valid(b'/test/a'))
    print('create_revision of /test/a : ', c.kv_create_revision(b'/test/a'))
    print('mod_revision of /test/a : ', c.kv_mod_revision(b'/test/a'))
    print('version of /test/a : ', c.kv_version(b'/test/a'))
    print('lease of /test/a : ', c.kv_lease(b'/test/a'))

    # lease test
    l_id = c.lease_grant(0, 10)
    print(l_id)
    c.kv_put(b'/test/l', b'2', l_id)
    print('get key: /test/l : ', c.kv_get(b'/test/l'))
    time.sleep(30)
    print('get key /test/l after lease : ', c.kv_get(b'/test/l'))

