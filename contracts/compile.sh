#！/bin/bash
cat Etcd.sol | solc --bin --abi --optimize - > Etcd.bin
