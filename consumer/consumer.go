package consumer

import (
	"errors"
	"fmt"
	"github.com/15125505/zlog/log"
	"github.com/Leafney/giraffe/utils"
	"github.com/streadway/amqp"
	"time"
)

/*
参考自：https://github.com/streadway/amqp/issues/133#issuecomment-102842493


*/

type Consumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	amqpURI       string     // amqp连接地址
	exchangeName  string     // 交换器名称
	exchangeType  string     // 交换器支持的类型
	durable       bool       // 消息是否持久化
	prefetchCount int        // 每次获取n条消息
	consumerTag   string     // client名称
	autoAck       bool       // 是否自动确认
	closeErr      chan error // 关闭异常
	queueName     string     // 队列名称
	bingdingKey   string     // 路由键

}

func NewConsumer(uri, consumerTag, exchangeName, exchangeType string, durable bool, prefetchCount int, autoAck bool) *Consumer {
	return &Consumer{
		amqpURI:       uri,
		exchangeName:  exchangeName,
		exchangeType:  exchangeType,
		durable:       durable,
		prefetchCount: prefetchCount,
		consumerTag:   consumerTag,
		autoAck:       autoAck,
		closeErr:      make(chan error),
	}
}

// 创建连接
func (c *Consumer) Connect() error {
	var err error
	c.conn, err = amqp.Dial(c.amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	// 后台协程检测异常关闭
	go func() {
		chanErr := <-c.conn.NotifyClose(make(chan *amqp.Error))
		log.Error(fmt.Sprintf("closing for: %s", chanErr))
		// 当 chanErr 为 nil 时，表示正常关闭；当!=nil 时，表示异常关闭
		if chanErr != nil {
			c.closeErr <- errors.New("Channel Closed")
		}
	}()

	log.Info("got Connection,getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	log.Info(fmt.Sprintf("got Channel,declaring Exchange (%q)", c.exchangeName))
	if err = c.channel.ExchangeDeclare(
		c.exchangeName,
		c.exchangeType,
		c.durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}
	return nil
}

// 接收消息
func (c *Consumer) AnnounceQueue(queueName, bindingKey string) (<-chan amqp.Delivery, error) {

	c.queueName = queueName
	c.bingdingKey = bindingKey

	log.Info(fmt.Sprintf("declared Exchange,declaring Queue %q", queueName))
	queue, err := c.channel.QueueDeclare(
		queueName,
		c.durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Info(fmt.Sprintf("declared Queue (%q %d message, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, bindingKey))

	//	Qos
	if err = c.channel.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("Error setting qos: %s", err)
	}

	if err = c.channel.QueueBind(
		queue.Name,
		bindingKey,
		c.exchangeName,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Info(fmt.Sprintf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.consumerTag))
	deliveries, err := c.channel.Consume(
		queue.Name,
		c.consumerTag,
		c.autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	return deliveries, nil
}

// 处理消息
// d: 接收到的消息内容
// fn: 处理消息的方法
// threads: 限定处理消息的协程数
// delayMaxSec: 消息处理完成后是否延迟ack,延迟时间随机在 1~~delayMaxSec 之间,0表示不延迟ack
func (c *Consumer) Handle(d <-chan amqp.Delivery, fn func(amqp.Delivery) error, threads int, delayMaxSec int) {
	if c.queueName == "" || c.bingdingKey == "" {
		log.Error("must set queueName and bindingKey")
		panic("must set queueName and bindingKey") // Todo 修改成默认值
	}

	var err error
	for {
		// multiple goroutine
		for i := 0; i < threads; i++ {
			go handleLoop(d, fn, c.autoAck, delayMaxSec)
		}

		//	go into reconnect loop when c.done is passed non nil values
		if <-c.closeErr != nil {
			var m = 1
			for {
				d, err = c.ReConnect(c.queueName, c.bingdingKey)
				if err != nil {
					if m < 6 {
						// 连接失败，有三次 60s 180s 300s 间隔的重试。之后如果仍然链接失败，则等待10分钟
						t := 60 * m
						log.Error(fmt.Sprintf("Reconnecting Error and after %d s retry...", t))
						time.Sleep(time.Duration(t) * time.Second)
					} else {
						log.Error(fmt.Sprintf("Reconnecting Error: %s", err))
						// 这里重连失败，直接return，调用外面的重连机制
						return
					}
				} else {
					break
				}
				m = m + 2
			}
		}
		// 到这里肯定是重连成功了
		log.Info("Reconnected... possibly")
	}
}

func (c *Consumer) ReConnect(queueName, bindingKey string) (<-chan amqp.Delivery, error) {

	//log.Info("waitting...30s...")
	//time.Sleep(30 * time.Second)

	if err := c.Connect(); err != nil {
		log.Error(fmt.Sprintf("Could not connect in reconnect call: %v", err.Error()))
	}
	deliveries, err := c.AnnounceQueue(queueName, bindingKey)
	if err != nil {
		return deliveries, errors.New("Could not connect")
	}
	return deliveries, nil
}

// 处理
func handleLoop(msgs <-chan amqp.Delivery, handlerFunc func(amqp.Delivery) error, autoAck bool, delayMaxSec int) {
	for d := range msgs {
		// 对返回结果执行相应操作
		err := handlerFunc(d)

		// 根据方法处理结果，向mq返回处理结果ack
		if !autoAck {
			// 不自动确认，等待处理成功后再确认
			// ack on success
			if err == nil {
				//在ack之前设置等待时间，起到消峰限流的效果
				if delayMaxSec > 0 {
					sec := utils.GetRandInt(delayMaxSec)
					log.Info(fmt.Sprintf("Sleep for %d seconds before delivery acknowledgement.", sec))
					time.Sleep(time.Second * time.Duration(sec))
				}
				// 确认收到本条消息, multiple必须为false
				d.Ack(false)
			} else {
				// Todo Nack容易导致消息出错，重新返回队列中，下一次处理仍出错，又会被返回队列中而先陷入死循环
				d.Nack(false, true)
			}
		}
	}
}

// 断开连接
func (c *Consumer) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
