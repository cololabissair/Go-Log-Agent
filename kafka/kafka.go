package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

// logData 主要是为了将tail读取的信息封装起来
type logData struct {
	topic string
	data  string
}

var (
	client  sarama.SyncProducer
	logChan chan *logData //taillog会将读取到的信息，发往这个管道，kafka在这个管道接收信息
)

func Init(address []string) (err error) {
	config := sarama.NewConfig()
	//配置kafka
	config.Producer.RequiredAcks = sarama.WaitForAll          //集群中所有follow和leader都回复ACK才进行返回
	config.Producer.Partitioner = sarama.NewRandomPartitioner //随机一个分区
	config.Producer.Return.Successes = true                   //成功交付消息将在success_channel返回
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		return
	}
	//对logchan进行初始化
	logChan = make(chan *logData, 100000) //初始化一个缓冲区足够大的管道

	//初始化时就直接开启协程，读取管道
	go ReadChanSendToKafka()
	return
}

// SendLog 将taillog包获取到的日志，塞进全局的管道logChan，以供读取
func SendToLogChan(topic, msg string) {
	logdata := &logData{
		topic: topic,
		data:  msg,
	}
	logChan <- logdata
}

// ReadChanSendToKafka 从管道中读取日志信息，并将信息发送至kafka
func ReadChanSendToKafka() {
	//构造一个消息
	msg := &sarama.ProducerMessage{}
	for {
		select {
		case logdata := <-logChan:
			msg.Topic = logdata.topic
			msg.Value = sarama.StringEncoder(logdata.data)
			//发送给kafka
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send message to kafka failed err:", err)
				continue
			}
			fmt.Printf("pid:%v  offset:%v \n", pid, offset)
			fmt.Println("send message success!")
		default:
			time.Sleep(time.Second)
		}
	}
}
