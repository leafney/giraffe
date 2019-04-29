package mqs

import (
	"encoding/json"
	"fmt"
	"github.com/15125505/zlog/log"
	"github.com/leafney/giraffe/publisher"
)

var pub *publisher.Publisher

// 初始化
func InitConnect(uri, consumerTag, exchangeName, exchangeType string, durable bool) {
	// 通过defer recover 来恢复panic异常
	defer func() {
		if err := recover(); err != nil {

		}
	}()
	//Todo 从配置文件中获取MQ配置信息
	pub = publisher.NewPublisher(
		uri, consumerTag, exchangeName, exchangeType, durable,
	)

	if err := pub.Connect(); err != nil {
		panic(err)
	}

	pub.KeepConnecting()
}

// 断开连接
func CloseConnect() {
	pub.Close()
}

// 将任务发布到指定队列-通用方法-Json
func MQSendJsonTask(co interface{}, queueName string, routingKey string) {
	body, _ := json.Marshal(co)

	go pub.SendingToQueue(queueName, routingKey, "json", []byte(body))

	msg := fmt.Sprintf("mq sending to queue:[%s] with key:[%s] for data:[%s] ok", queueName, routingKey, body)
	log.Info(msg)
}

// 将任务发布到指定队列-通用方法-Text
func MQSendTextTask(body string, queueName string, routingKey string) {

	go pub.SendingToQueue(queueName, routingKey, "text", []byte(body))

	msg := fmt.Sprintf("mq sending to queue:[%s] with key:[%s] for data:[%s] ok", queueName, routingKey, body)
	log.Info(msg)
}
