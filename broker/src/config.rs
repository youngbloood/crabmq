#[derive(Clone)]
pub struct Config {
    pub id: u32,

    // borker 监听的 grpc 地址
    pub broker_addr: String,

    // 产生 BrokerState 的时间间隔，单位: s
    pub report_broker_state_interval: u64,

    // 客户端生产消息时的内存缓冲区大小
    pub publish_buffer_size: usize,

    // 客户端生产消息时的内存缓冲区大小
    pub subscribe_buffer_size: usize,

    // 每个订阅 state_bus 的缓冲区大小
    pub state_bus_buffer_size: usize,

    // 每个订阅者默认超时时间，单位: s
    pub subscriber_timeout: u64,

    // 每个订阅 message_bus 生产者消息的缓冲区大小
    pub message_bus_producer_buffer_size: usize,

    // 每个订阅 message_bus 消费者消息的缓冲区大小
    pub message_bus_consumer_buffer_size: usize,
}

impl Config {
    pub fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    pub fn with_broker_addr(mut self, broker_addr: String) -> Self {
        self.broker_addr = broker_addr;
        self
    }
}

pub fn default_config() -> Config {
    Config {
        id: 0,
        broker_addr: String::new(),
        report_broker_state_interval: 5,
        publish_buffer_size: 1024,
        subscribe_buffer_size: 1024,
        state_bus_buffer_size: 10,
        subscriber_timeout: 30,
        message_bus_producer_buffer_size: 1024,
        message_bus_consumer_buffer_size: 1024,
    }
}
