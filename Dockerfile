FROM xwharbor.wxchina.com/cpaas/component/ubuntu:24.10

COPY ./target/release/flink-kafka-filter-transform /opt

WORKDIR /opt

ENTRYPOINT ["/opt/flink-kafka-filter-transform"]