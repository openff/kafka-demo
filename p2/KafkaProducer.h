#ifndef KAFKAPRODUCER_H
#define KAFKAPRODUCER_H

#pragma once
#include <string>
#include <iostream>
#include <librdkafka/rdkafkacpp.h>
// 消息发送结果的回调函数 dr_cb。
class ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message)
    {
        if (message.err()) // 检查消息是否发送成功 返回非零值,表示消息发送失败,
            std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
        else // Message delivered to topic test [0] at offset 135000
            std::cerr << "Message delivered to topic " << message.topic_name()
                      << " [" << message.partition() << "] at offset "
                      << message.offset() << std::endl;
    }
};
// 生产者事件的回调函数
class ProducerEventCb : public RdKafka::EventCb
{
public:
    void event_cb(RdKafka::Event &event)
    {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR: // 打印错误信息
            std::cout << "RdKafka::Event::EVENT_ERROR: " << RdKafka::err2str(event.err()) << std::endl;
            break;
        case RdKafka::Event::EVENT_STATS: // 打印统计信息
            std::cout << "RdKafka::Event::EVENT_STATS: " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG: // 打印日志信息
            std::cout << "RdKafka::Event::EVENT_LOG " << event.fac() << std::endl;
            break;
        case RdKafka::Event::EVENT_THROTTLE: // 打印节流信息
            std::cout << "RdKafka::Event::EVENT_THROTTLE " << event.broker_name() << std::endl;
            break;
        }
    }
};
// 自定义分区器的回调函数 partitioner_cb。
class HashPartitionerCb : public RdKafka::PartitionerCb
{
public:
    // 参数:主题（一个生产者可以给投递多个主题） key 分区数量  扩展信息
    int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
                           int32_t partition_cnt, void *msg_opaque)
    {
        char msg[128] = {0};
        // 根据消息的 key 值计算一个哈希值
        int32_t partition_id = generate_hash(key->c_str(), key->size()) % partition_cnt;
        //                          [topic][key][partition_cnt][partition_id]
        //                          :[test][6419][2][1]
        // 根据分区数量和哈希值计算分区 ID
        sprintf(msg, "HashPartitionerCb:topic:[%s], key:[%s]partition_cnt:[%d], partition_id:[%d]", topic->name().c_str(),
                key->c_str(), partition_cnt, partition_id);
        std::cout << msg << std::endl;
        return partition_id;
    }

private:
    static inline unsigned int generate_hash(const char *str, size_t len)
    {
        unsigned int hash = 5381;
        for (size_t i = 0; i < len; i++)
            hash = ((hash << 5) + hash) + str[i];
        return hash;
    }
};
/*
KafkaProducer 类:
构造函数接受 Kafka 集群的 broker 列表、Topic 名称和分区号作为参数。
在构造函数中创建 Kafka 生产者配置对象 m_config 和 Topic 配置对象 m_topicConfig。
创建 Kafka 生产者对象 m_producer 和 Topic 对象 m_topic。
注册上述三个回调类的实例到生产者和 Topic 配置对象中。
*/
class KafkaProducer
{
public:
    /**
     * @brief KafkaProducer
     * @param brokers
     * @param topic
     * @param partition
     */
    explicit KafkaProducer(const std::string &brokers, const std::string &topic,
                           int partition);
    /**
     * @brief push Message to Kafka
     */
    void pushMessage(const std::string &str, const std::string &key);
    ~KafkaProducer();

protected:
    template <typename KEY, typename VAL>
    void setConfigParameter(const KEY &key, const VAL &value, RdKafka::Conf *conf = nullptr)
    {
        RdKafka::Conf *target = conf ? conf : m_config;
        std::string errorStr;

        RdKafka::Conf::ConfResult errCode = target->set(key, value, errorStr);
        if (errCode != RdKafka::Conf::CONF_OK)
        {
            std::cerr << "Failed to set config parameter [" << key << "]: " << errorStr << std::endl;
        }
    }
    /**
     * @brief 设置通用 Kafka 配置参数
     */
    void setCommonConfig()
    {
        // broker 的地址清单
        setConfigParameter<std::string, std::string>("bootstrap.servers", m_brokers);
        // 自动提交offset之间的间隔毫秒数
        setConfigParameter<std::string, std::string>("statistics.interval.ms", "10000");
        // 客户端能够发送的消息的最大值 1048576 B 等于 1 MB   此处设置5Mb
        setConfigParameter<std::string, std::string>("message.max.bytes", "5242880");
        // Socket 接受消息缓冲区（SO_RECBUF）的大小 设置为 -1，则使用操作系统的默认值。 1M
        setConfigParameter<std::string, std::string>("socket.receive.buffer.bytes", "1048576");
        // Socket 发送消息缓冲区（SO_SNDBUF）的大小
        setConfigParameter<std::string, std::string>("socket.send.buffer.bytes", "1048576");
    }
    /**
     * @brief 设置生产者特定的 Kafka 配置参数
     */
    void setProducerConfig()
    {
        
        // 重连策略 start
        // 消息处理失败时的最大重试次数 因为中间重试会出现“数据乱序现象” 配合 max.in.flight.requests.per.connection = 1 保证 但是吞吐量降低
        setConfigParameter<std::string, std::string>("retries", "3");
        // 消息处理失败时的最大  重试间隔time ms
        setConfigParameter<std::string, std::string>("retry.backoff.ms", "7000");
        // 单线程发送消息 保证数据顺序
        setConfigParameter<std::string, std::string>("max.in.flight.requests.per.connection", "1");
        // 重连策略 end

        // 折中方案 生产者发送消息之后，只要分区的 leader 副本成功写入消息，那么它就会收到来自服务端的成功响应。
        setConfigParameter<std::string, std::string>("acks", "1");
        // 压缩方式 默认值为 “none”  or  “gzip”，“snappy”，“lz4”。 性能： LZ4 > Snappy > GZIP
        setConfigParameter<std::string, std::string>("compression.type", "none");
        // 批处理发送大小  默认值是 16384 ，即16KB   512kb
        //setConfigParameter<std::string, std::string>("batch.size", "524288");
        // 批处理发送延时 增加消息的延迟，但是同时能提升一定的吞吐量。
        setConfigParameter<std::string, std::string>("linger.ms", "420000");
        // Producer 等待请求响应的最长时间，默认值为 30000 ms。
        setConfigParameter<std::string, std::string>("request.timeout.ms", "30000");
        // request.required.acks  0=Broker does not send any response/ack to client,
        //                        -1 or all=Broker will block until message is committed by all in sync replicas (ISRs)
    }

    /**
     * @brief 创建 Kafka 生产者对象
     */
    void createProducer() {
        std::string errorStr;
        m_producer = RdKafka::Producer::create(m_config, errorStr);
        if (m_producer == nullptr) {
            throw std::runtime_error("Failed to create Kafka producer: " + errorStr);
        }
    }
     /**
     * @brief 创建 Kafka Topic 对象
     */
    void createTopic() {
        std::string errorStr;
        // 创建Topic对象
        /*
        参数：
        生产者对象(m_producer)
        Topic 名称(m_topicStr)
        Topic 级别的配置对象(m_topicConfig)
        用于存储错误信息的字符串引用(errorStr)
        */
        m_topic = RdKafka::Topic::create(m_producer, m_topicStr, m_topicConfig, errorStr);
        if (m_topic == nullptr) {
            throw std::runtime_error("Failed to create Kafka topic: " + errorStr);
        }

        
    }
    std::string m_brokers;        // Broker列表，多个使用逗号分隔
    std::string m_topicStr;       // Topic名称
    int m_partition;              // 分区
    RdKafka::Conf *m_config;      // Kafka Conf对象
    RdKafka::Conf *m_topicConfig; // Topic Conf对象
    int partition_count = 1;    //默认1分区

    RdKafka::Topic *m_topic;       // Topic对象
    RdKafka::Producer *m_producer; // Producer对象
    RdKafka::DeliveryReportCb *m_dr_cb;
    RdKafka::EventCb *m_event_cb;
    RdKafka::PartitionerCb *m_partitioner_cb; // 只要看到Cb 结尾的类，要继承它然后实现对应的回调函数
};

#endif // KAFKAPRODUCER_H
