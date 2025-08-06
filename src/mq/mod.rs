use std::sync::Arc;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};
use tokio::sync::Mutex;

use crate::config::transform::FlinkKafkaTransform;

pub mod kafka;

async fn metrics_registry(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;
    let metrics = Metrics::default();
    metrics.register(&mut registry);

    return metrics;
}

pub async fn consumer_kafka(registry: Arc<Mutex<Registry>>) {
    let metrics = metrics_registry(registry).await;
    let transform = FlinkKafkaTransform::new("config.yaml".to_string());

    let metrics = Arc::new(metrics);
    let transform = Arc::new(transform);
    kafka::kafka_transform(transform, metrics).await;
}

///
/// 这个labels主要是记录朝向目标Topic投递的数据统计
/// topic:目标数据的Topic
/// op:对应的操作类型(c/u/d)
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TransformLables {
    topic: String,
    op: String,
}

///
/// 这个labels主要是记录Flink CDC Event的类型统计
/// topic:源数据的Topic
/// db:对应的数据库
/// table:对应的表
/// op:对应的操作类型(c/u/d)
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EventLables {
    topic: String,
    db: String,
    table: String,
    op: String,
}

#[derive(Debug)]
pub(crate) struct Metrics {
    flink_kafka_filter_transform: Family<TransformLables, Counter>,
    flink_cdc_event: Family<EventLables, Counter>,
}

impl Metrics {
    pub fn default() -> Self {
        Metrics {
            flink_kafka_filter_transform: Family::default(),
            flink_cdc_event: Family::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "flink_kafka_filter_transform_count",
            "flink kafka filter transform count size",
            self.flink_kafka_filter_transform.clone(),
        );
        registry.register(
            "flink_cdc_event_count",
            "Flink CDC Event count size",
            self.flink_cdc_event.clone(),
        );
    }

    pub fn inc_flink_kafka_filter_transform(&self, topic: String, op: String) {
        self.flink_kafka_filter_transform
            .get_or_create(&TransformLables {
                topic: topic,
                op: op,
            })
            .inc();
    }

    pub fn inc_flink_cdc_event(&self, topic: String, db: String, table: String, op: String) {
        self.flink_cdc_event
            .get_or_create(&EventLables {
                topic,
                db,
                table,
                op,
            })
            .inc();
    }
}
