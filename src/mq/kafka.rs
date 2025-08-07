use std::sync::Arc;

use futures::TryStreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
use serde::Deserialize;
use tracing::{info, warn};

use crate::{config::transform::FlinkKafkaTransform, mq::Metrics};

const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
const GROUP_ID: &str = "group.id";
const AUTO_OFFSET_RESET: &str = "auto.offset.reset";
const EARLIEST: &str = "earliest";
const MESSAGE_TIMEOUT_MS: &str = "message.timeout.ms";

pub(crate) async fn kafka_transform(transform: Arc<FlinkKafkaTransform>, metrics: Arc<Metrics>) {
    async_processor(transform, metrics).await;
}

///
/// 采用官方网站的方式，编写async的消费和发送函数
/// 目前按照这个速度，大概能跑到:8.5w/s的速度，但是内存需要占据到1.7GB左右,CPU大概是:3.52C
/// 总体来说速度和消耗的资源基本上匹配，但是内存和CPU还是有点高了
///
async fn async_processor(transform: Arc<FlinkKafkaTransform>, metrics: Arc<Metrics>) {
    let consumer = ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, transform.bootstrap_servers())
        .set(GROUP_ID, transform.group())
        .set(AUTO_OFFSET_RESET, EARLIEST)
        .set("session.timeout.ms", "6000")
        .create::<StreamConsumer>()
        .expect("consumer creation failed");
    consumer
        .subscribe(transform.binding_topics().as_slice())
        .expect("subscribe failed,check the topic name");

    let producer = ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, transform.bootstrap_servers())
        .set(MESSAGE_TIMEOUT_MS, "5000")
        .set("batch.size", "10485760")
        .create::<FutureProducer>()
        .expect("producer creation failed");

    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let transform = transform.clone();
        let metrics = metrics.clone();
        async move {
            let debezium_json =
                serde_json::from_slice::<DebeziumJson>(&borrowed_message.payload().unwrap())
                    .unwrap();
            metrics.inc_flink_cdc_event(
                borrowed_message.topic().to_string(),
                debezium_json.db().to_string(),
                debezium_json.table().to_string(),
                debezium_json.op().to_string(),
            );

            let message = borrowed_message.detach();
            tokio::spawn(async move {
                match debezium_json.op.as_str() {
                    "d" => {
                        info!("delete event discard!");
                    }
                    _ => {
                        if let Some(target_topic) = transform.search_topic(
                            message.topic(),
                            debezium_json.db(),
                            debezium_json.table(),
                        ) {
                            metrics.inc_flink_kafka_filter_transform(
                                target_topic.to_string(),
                                debezium_json.op.to_string(),
                            );

                            let send_record = FutureRecord::to(&target_topic)
                                .key(message.key().unwrap())
                                .payload(message.payload().unwrap());

                            //
                            //看到这块在rdkafka上有个bug:https://github.com/fede1024/rust-rdkafka/issues/746
                            //使用producer.send().await的时候，会出现内存的泄漏，主要表现为:内存逐步的增长,不会降低
                            //目前看采用send_result的方式，在11w/s的时候，内存占用大概：380MB，消费速度在2w/s的时候，内存占用大概是:35MB
                            let result = producer.send_result(send_record);

                            match result {
                                Ok(_) => {
                                    info!(
                                        "send message success:{} db:{} table:{}!",
                                        &target_topic,
                                        debezium_json.db(),
                                        debezium_json.table()
                                    );
                                }
                                Err(e) => {
                                    warn!("send message error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            });
            Ok(())
        }
    });

    info!("start kafka consumer stream");
    stream_processor.await.expect("stream processor error");
    info!("Stream processing terminated");
}

///
/// 提供debezium的json格式的CDC数据结构对象
///
#[derive(Debug, Deserialize)]
struct DebeziumJson {
    op: String,
    source: Source,
}

impl DebeziumJson {
    pub fn db(&self) -> &str {
        self.source.db()
    }

    pub fn table(&self) -> &str {
        self.source.table()
    }

    pub fn op(&self) -> &str {
        self.op.as_str()
    }
}

#[derive(Debug, Deserialize)]
struct Source {
    db: String,
    table: String,
}

impl Source {
    pub fn db(&self) -> &str {
        self.db.as_str()
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }
}

#[cfg(test)]
mod tests {
    use tracing::{error, info};

    use crate::LocalTimer;

    use super::*;

    fn init() {
        tracing_subscriber::fmt().with_timer(LocalTimer).init();
    }

    #[test]
    fn test_parse_debezium_json() {
        init();
        let json = r#"
        {
    "before": null,
    "after": {
        "id": 62,
        "update_time:"2025-08-07 13:37:45"
    },
    "op": "c",
    "source": {
        "db": "db_1",
        "table": "table_1"
    }
}
        "#;

        let debezium_json = serde_json::from_str::<DebeziumJson>(json);
        match debezium_json {
            Ok(debezium_json) => {
                info!("debezium_json: {:?}", debezium_json);
            }
            Err(err) => {
                error!("debezium_json: {:?}", err);
            }
        }
    }
}
