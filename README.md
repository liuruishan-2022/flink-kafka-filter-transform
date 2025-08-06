# 1. 概述

本服务的设计思路是：因为 flink cdc 3 通过 debezium 投递给 Kafak 的消息支持:insert/update/delete 的 CDC,但是 Doris 的 Routine Load 目前只支持处理 insert/update,对于 delete 无法支持，原因如下:

insert/update:

```json
{
  "before": null,
  "after": {
    ...
  },
  "op": "c或者u:c是create的意思,u是update的意思",
  "source": {
    "db": "db_1",
    "table": "table_1"
  }
}
```

delete 的 debezium 的报文如下:

```json
{
  "before": {
    ...
  },
  "after": null,
  "op": "d",
  "source": {
    "db": "db_1",
    "table": "table_1"
  }
}
```

Doris 的 Routine Load 目前只能做到入口解析，后续如果想在 load 的时候，根据不同的 op 做不同的动作，无法再次解析 JSON,Doris 的 Routine Load 应该是
考虑到了性能的问题.

# 2. 需要实现的功能

1. 消费指定的 Topic，过滤掉 op: d(delete)的 CDC 事件
2. 根据 source.db 和 source.table,然后执行过滤的操作
3. 然后执行 transform 之后,然后转发给另外一个 Topic

大致的流程如下:

kafka--->flink-kafka-filter-transform---->kafka

目前暂时只支持相同 Kafka 的集群的投递，不支持从一个 Kafka 集群投递给另外一个 Kafka 集群
