#ifndef KAFKACONSUMER_H
#define KAFKACONSUMER_H

#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <stdio.h>
#include<librdkafka/rdkafkacpp.h>
//处理 Kafka 消费者相关的事件回调。
class ConsumerEventCb : public RdKafka::EventCb
{
public:
    void event_cb (RdKafka::Event &event)
    {
        switch (event.type())
        {
        /*
            对于错误事件,会打印出错误信息,并标明是否是致命错误。
            对于统计信息事件,会直接打印出统计信息。
            对于日志事件,会打印出日志信息,包括日志级别和日志源。
            对于节流事件,会打印出节流的时长和节流施加的 Broker 信息。
        */
        case RdKafka::Event::EVENT_ERROR:   //发生错误事件
            if (event.fatal())
            {
                std::cerr << "FATAL ";
            }
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                      event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_STATS:   //收到统计信息事件
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_LOG: //收到日志事件
            fprintf(stderr, "LOG-%i-%s: %s\n",
                    event.severity(), event.fac().c_str(), event.str().c_str());
            break;

        case RdKafka::Event::EVENT_THROTTLE:    //收到节流事件
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
                      event.broker_name() << " id " << (int)event.broker_id() << std::endl;
            break;

        default:
            std::cerr << "EVENT " << event.type() <<
                      " (" << RdKafka::err2str(event.err()) << "): " <<
                      event.str() << std::endl;
            break;
        }
    }
};
/*
消费组中的消费者会动态地加入或退出,导致分区的所有权发生变化。这个过程称为分区重新平衡(Rebalance)。
当分区重新平衡发生时,Kafka 客户端需要做出相应的处理,以确保消费者能够正确地消费分区中的消息
*/
class ConsumerRebalanceCb : public RdKafka::RebalanceCb
{
private:
    //打印当前获取的分区信息。
    static void printTopicPartition (const std::vector<RdKafka::TopicPartition*>&partitions)        // 打印当前获取的分区
    {
        for (unsigned int i = 0 ; i < partitions.size() ; i++)
            std::cerr << partitions[i]->topic() <<
                      "[" << partitions[i]->partition() << "], ";
        std::cerr << "\n";
    }

public:
    /**
     * brif:用于处理分区重新平衡事件
     *  parm:
     * consumer: 当前的 RdKafka::KafkaConsumer 实例
    err: 表示分区重新平衡结果的错误码
    partitions: 表示分配给当前消费者的分区列表
    */
    void rebalance_cb (RdKafka::KafkaConsumer *consumer,
                       RdKafka::ErrorCode err,
                       std::vector<RdKafka::TopicPartition*> &partitions)
    {
        std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";
        printTopicPartition(partitions); //打印出当前获取到的分区信息。
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS)  //需要分配分区
        {
            //将分区分配给消费者。同时记录下当前分区的数量。 ps:订阅了多个分区，可能你属于不同的主题
            consumer->assign(partitions);
            partition_count = (int)partitions.size();
        }
        else    //需要取消分区分配
        {
            consumer->unassign();
            partition_count = 0;
        }
    }
private:
    int partition_count;
};

class KafkaConsumer
{
public:/**
     * @brief KafkaConsumer
     * @param brokers
     * @param groupID
     * @param topics
     * @param partition
     */
    explicit KafkaConsumer(const std::string& brokers, const std::string& groupID,
                           const std::vector<std::string>& topics, int partition);
    void pullMessage();
    ~KafkaConsumer();
protected:
    std::string m_brokers;
    std::string m_groupID;
    std::vector<std::string> m_topicVector;
    int m_partition;
    RdKafka::Conf* m_config;
    RdKafka::Conf* m_topicConfig;
    RdKafka::KafkaConsumer* m_consumer;
    RdKafka::EventCb* m_event_cb;
    RdKafka::RebalanceCb* m_rebalance_cb;
};

#endif // KAFKACONSUMER_H
