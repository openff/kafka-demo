#include <iostream>
#include "KafkaProducer.h"
using namespace std;

int main()
{
    // 创建Producer
    // KafkaProducer producer("127.0.0.1:9092,192.168.2.111:9092", "test", 0);
    KafkaProducer producer("127.0.0.1:9092", "test8", 0);
    
    for(int i = 0; i < 10000; i++)
    {
        char msg[64] = {0};
        sprintf(msg, "%s%4d", "Hello RdKafka ", i);
        // 生产消息
        char key[8] = {0};      // 主要用来做负载均衡
        sprintf(key, "%d", i);
        producer.pushMessage(msg, key);  
        // char *ptr = "";   
        // producer.pushMessage(msg, ptr);
    }
    RdKafka::wait_destroyed(5000);
}
