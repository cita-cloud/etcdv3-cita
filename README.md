# etcdv3-cita

作为融合`CITA`和`Kubernetes`的第一步，我们计划是将`CITA`适配成`etcd`接口。目前最新的`Kubernetes`使用的是`etcdV3`接口。

## etcdv3

[仓库](https://github.com/etcd-io/etcd)

`etcdv3`接口是用`gRPC`定义的，`proto`文件[链接](https://github.com/etcd-io/etcd/blob/master/etcdserver/etcdserverpb/rpc.proto)。

## 适配计划

本项目会作为一个单独的服务，对接收到的`etcd`请求进行解析和转换，然后调用后端的`CITA`完成相关的任务。

因为两者功能和接口差异比较大。因此部分功能需要以智能合约的方式实现，并事先部署到`CITA`链上，相关功能的请求转换为调用智能合约。

两者还有一个比较大的区别是`CITA`有出块间隔，因此进行了`put`等操作之后，无法很快响应查询。可能需要在本项目中对一些操作和结果进行缓存，提高响应时间。另外一个可选方案是将`CITA`出块间隔调小，比如到一秒，同时调大`Kubernetes`那边的超时时间。

另外，本项目主要目的是验证技术可行性，不追求完美模拟完整的`etcdv3`接口，以运行`Kubernetes`基本功能为目标。

## 详细设计

整个工作分两部分，一部分是适配服务，主要是模拟`etcd`提供的`gRPC`接口；另外一部分是在`CITA`上实现`etcd`对应的功能。最后将两者对接。

#### 提取`proto`文件

`etcd`的`rpc`接口有单独一个`proto`文件描述。但是这个文件的依赖很多，跟整个`etcd`工程绑定很紧密。

需要首先把它单独提取出来，然后生成对应的`python`代码。

#### 搭建`gRPC`服务器

本项目使用`python`实现，使用已有的库构建`gRPC`服务器，实现`etcd`所有的`rpc`接口。具体实现可以先插桩，后续根据和`Kubernetes`的联调情况再看具体需要实现哪些接口。

#### 合约模拟`etcd`功能

`etcd`提供的功能远远超出了单纯的`KV`存储，这些额外的功能`CITA`都没有可以直接对应的功能。目前考虑使用智能合约进行模拟，每一大类功能用一个合约来实现。


## 工作进展

- `etcd`的`proto`文件提取完成，成功生成了对应`grpc`的`python`接口代码。
- 实现了转发`k8s`和`etcd`通信的中间服务器，用以观察它们之间通信协议(`py/inspector.py`)。
- 在`CITA`上实现了部分所需的功能的智能合约。
- 用`Python`构建了`gRPC`服务器，调用后端`CITA`实现`k8s`的`api-server`使用的`gRPC`接口，
经上一步的观察发现启动步骤主要用的是`Range`, `Put`, `Watch`, `Txn`这几个功能。
- 手工启动`k8s`，使用伪装的服务器替代`etcd`，观察`kube-apiserver`的运行情况，
同时对比使用了真实的`etcd`和安插了转发服务器的`etcd`的`apiserver`日志。
- 最终发现伪装`etcd`的服务器能达到和安插转发器的`etcd`的`apiserver`日志情况相似，
但两者均存在报错出现（连接超时等），而使用了真实的`etcd`的`apiserver`没有报错产生，
初步认为是`etcd`中存在着一些未知的接口没有实现，可能是由于处理`gRPC`的`proto`文件的时候去掉了部分拓展插件的原因。
