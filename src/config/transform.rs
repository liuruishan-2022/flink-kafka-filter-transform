use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct FlinkKafkaTransform {
    kafka: Kafka,
    transforms: Vec<Transform>,
    #[serde(skip)]
    regex_transforms: Vec<RegexTransform>,
}

impl FlinkKafkaTransform {
    //
    //从指定的路径读取配置文件，反解析成FlinkKafkaTransform的实体
    pub fn new(config_path: String) -> Self {
        let config_file = std::fs::read_to_string(config_path).expect("Unable to read config file");

        let mut config: FlinkKafkaTransform = serde_yaml::from_str(&config_file)
            .expect("Unable to parse config file to pub struct FlinkKafkaTransform");

        config.regex_transforms = FlinkKafkaTransform::init_regex_transforms(&config);

        config
    }

    fn init_regex_transforms(config: &FlinkKafkaTransform) -> Vec<RegexTransform> {
        return config
            .transforms
            .iter()
            .map(|ele| -> RegexTransform {
                let clone_transform = ele.clone();
                return RegexTransform {
                    regex: Regex::new(ele.table()).unwrap(),
                    transform: clone_transform,
                };
            })
            .collect();
    }

    pub fn bootstrap_servers(&self) -> &str {
        self.kafka.bootstrap_servers()
    }

    pub fn binding_topics(&self) -> Vec<&str> {
        self.kafka.binding_topics()
    }

    pub fn group(&self) -> &str {
        self.kafka.group()
    }

    ///
    /// 需要校验如下信息相等: source_topic,db,table(支持正则表达式的匹配)
    /// source_topic和db只支持绝对相等
    pub fn search_topic(&self, source_topic: &str, db: &str, table: &str) -> Option<&str> {
        return self
            .regex_transforms
            .iter()
            .find(|ele| -> bool {
                return ele.transform.source_topic == source_topic
                    && ele.transform.db == db
                    && ele.regex.is_match(table);
            })
            .map(|trans| trans.transform.target_topic());
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Kafka {
    bootstrap_servers: String,
    group: String,
    bindings: Vec<String>,
}

impl Kafka {
    pub fn bootstrap_servers(&self) -> &str {
        self.bootstrap_servers.as_str()
    }

    pub fn binding_topics(&self) -> Vec<&str> {
        self.bindings.iter().map(|s| s.as_str()).collect()
    }

    pub fn group(&self) -> &str {
        self.group.as_str()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Transform {
    source_topic: String,
    db: String,
    table: String,
    target_topic: String,
}

impl Transform {
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn target_topic(&self) -> &str {
        &self.target_topic
    }
}

pub struct RegexTransform {
    regex: Regex,
    transform: Transform,
}

#[cfg(test)]
mod tests {

    use regex::Regex;

    use crate::LocalTimer;

    use super::*;

    fn init() {
        tracing_subscriber::fmt().with_timer(LocalTimer).init();
    }

    #[test]
    fn test_read_flink_kafka_transform_config() {
        init();
        let config = FlinkKafkaTransform::new("config.yaml".to_string());
        info!("flink kafka transform config:{:?}", config);
    }

    #[test]
    fn test_regex_match() {
        init();
        let re = Regex::new(r"gsms_msg_ticket_sms_[0-9]+").unwrap();
        let text = "gsms_msg_ticket_sms_1009";
        let matched = re.is_match(text);
        info!(
            "re:{} matched text:{} result:{}!",
            re.as_str(),
            text,
            matched
        );

        let text = "gsms_msg_frame_0908";
        let matched = re.is_match(text);
        info!(
            "re:{} matched text:{} result:{}!",
            re.as_str(),
            text,
            matched
        );
    }

    #[test]
    fn test_match_target_topic() {
        let config = FlinkKafkaTransform::new("config.yaml".to_string());
        let result = config.search_topic("sedp_province_channel_monitor_plan");
        assert!(result.is_some());
        let result = config.search_topic("no_topic");
        assert!(result.is_none());
        let result = config.search_topic("gsms_msg_frame_mms_0908");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "test-kafka-sedp-gsms-msg-frame-mms");
    }
}
