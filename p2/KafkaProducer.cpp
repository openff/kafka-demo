#include "KafkaProducer.h"

KafkaProducer::KafkaProducer(const std::string &brokers, const std::string &topic, int partition)
{
    m_brokers = brokers;
    m_topicStr = topic;
    m_partition = partition;
    // 创建Kafka Conf对象
    m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (m_config == NULL)
    {
        std::cout << "Create RdKafka Conf failed." << std::endl;
        return;
    }
    // 创建Topic Conf对象
    m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (m_topicConfig == NULL)
    {
        std::cout << "Create RdKafka Topic Conf failed." << std::endl;
        return;
    }
    
 
    // 设置Broker属性
    RdKafka::Conf::ConfResult errCode;
    m_dr_cb = new ProducerDeliveryReportCb;
    std::string errorStr;

    setConfigParameter<std::string, RdKafka::DeliveryReportCb *>("dr_cb", m_dr_cb); // 异步方式发送
    // 生产者事件的回调函数
    m_event_cb = new ProducerEventCb;
    setConfigParameter<std::string, RdKafka::EventCb *>("event_cb", m_event_cb); 
    // 可根据主题name 选择不同的分配策略
    m_partitioner_cb = new HashPartitionerCb;
    setConfigParameter<std::string, RdKafka::PartitionerCb *>("partitioner_cb", m_partitioner_cb ,m_topicConfig); 

    // 分配策略2 ...
    setCommonConfig();
    setProducerConfig();
    // 创建Producer
    createProducer();
    createTopic();
}

void KafkaProducer::pushMessage(const std::string &str, const std::string &key)
{
    int32_t len = str.length();
    // 将消息数据转换为 void* 指针
    void *payload = const_cast<void *>(static_cast<const void *>(str.data()));
    // 调用 RdKafka::Producer::produce 函数发送消息
    /*
    RdKafka::Topic::PARTITION_UA: 未分配分区。这种情况下,Kafka 会根据内置的默认分区策略来选择分区。  指定分区 >  keyvalue策略 > robin
    RdKafka::Topic::PARTITION_RANDOM: 随机分区。Kafka 会随机选择一个分区来发送消息。
    RdKafka::Topic::PARTITION_CONSISTENT: 一致性分区。Kafka 会根据消息的 key 值计算出一个分区 ID,确保相同 key 的消息会被发送到同一个分区。
    RdKafka::Topic::PARTITION_CONSISTENT_RANDOM: 一致性随机分区。这是一种折中的方案,Kafka 会根据消息的 key 值计算出一个分区 ID,但如果该分区不可用,则会随机选择一个可用的分区。

    RdKafka::Producer::RK_MSG_COPY: 表示 librdkafka 库会复制消息的内容,以确保在回调函数中仍然可用。
    */
    RdKafka::ErrorCode errorCode = m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA,
                                                       RdKafka::Producer::RK_MSG_COPY,
                                                       payload, len, &key, 
                                                       NULL // 扩展信息 上下文传递 void *msg_opaque
    );
    // 用于执行内部的事件处理和消息发送完成的回调。传递 0 表示非阻塞调用。
    m_producer->poll(0);
    if (errorCode != RdKafka::ERR_NO_ERROR) // 检查消息发送结果
    {
        std::cerr << "Produce failed: " << RdKafka::err2str(errorCode) << std::endl;
        if (errorCode == RdKafka::ERR__QUEUE_FULL)
        {
            m_producer->poll(100); // 等待 100 毫秒后重试发送。
        }
    }
}

KafkaProducer::~KafkaProducer()
{
    while (m_producer->outq_len() > 0)
    {
        std::cerr << "Waiting for " << m_producer->outq_len() << std::endl;
        m_producer->flush(5000);
    }
    delete m_config;
    delete m_topicConfig;
    delete m_topic;
    delete m_producer;
    delete m_dr_cb;
    delete m_event_cb;
    delete m_partitioner_cb;
}
