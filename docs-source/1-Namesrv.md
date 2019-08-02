Namesrv
----------

NameServer是一个非常简单的Topic路由注册中心，其角色类似Dubbo中的zookeeper，支持Broker的动态注册与发现。主要包括两个功能：

- Broker管理，NameServer接受Broker集群的注册信息并且保存下来作为路由信息的基本数据。然后提供心跳检测机制，检查Broker是否还存活；

- 路由信息管理，每个NameServer将保存关于Broker集群的整个路由信息和用于客户端查询的队列信息。然后Producer和Conumser通过NameServer就可以知道整个Broker集群的路由信息，从而进行消息的投递和消费。

NameServer通常也是集群的方式部署，各实例间相互不进行信息通讯。Broker是向每一台NameServer注册自己的路由信息，所以每一个NameServer实例上面都保存一份完整的路由信息。当某个NameServer因某种原因下线了，Broker仍然可以向其它NameServer同步其路由信息，Producer,Consumer仍然可以动态感知Broker的路由的信息。

1. Broker启动的时候会注册到NameServer中，NameServer保存活跃的broker列表，提供心跳检测机制。
2. 用来保存所有topic和该topic所有队列的列表。
3. 保存所有broker的Filter列表。
4. 接收Producer和Consumer的请求，根据某个topic获取所有到broker的路由信息。



## NamesrvStartup

### NamesrvStartup启动过程

- NamesrvStartup解析命令行的一些参数到Config对象中，包括NamesrvConfig和NettyServerConfig等。也会尝试从配置文件中读取配置。
- 初始化NamesrvController。
- 配置shutdownHook。
- 启动NamesrvController。

### 初始化NamesrvController


# 参考

- [【RocketMQ源码学习】2-Namesrv](https://fdx321.github.io/2017/08/17/%E3%80%90RocketMQ%E6%BA%90%E7%A0%81%E5%AD%A6%E4%B9%A0%E3%80%912-Namesrv/)
- [RocketMQ——Name Server篇](https://blog.csdn.net/meilong_whpu/article/details/76922527)