package publisher

import (
	"errors"
	"fmt"
	"github.com/15125505/zlog/log"
	"github.com/streadway/amqp"
	"time"
)

type Publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	amqpURI      string     // amqp连接地址
	exchangeName string     // 交换器名称
	exchangeType string     // 交换器支持的类型
	durable      bool       // 消息是否持久化
	consumerTag  string     // client名称
	autoAck      bool       // 是否自动确认
	closeErr     chan error // 关闭异常
	queueName    string     // 队列名称
	routingKey   string     // 路由键
}

// 初始化
func NewPublisher(uri, consumerTag, exchangeName, exchangeType string, durable bool, autoAck bool) *Publisher {
	return &Publisher{
		amqpURI:      uri,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		durable:      durable,
		consumerTag:  consumerTag,
		autoAck:      autoAck,
		closeErr:     make(chan error),
	}
}

func (p *Publisher) KeepConnecting() {

	for {
		//	go into reconnect loop when c.done is passed non nil values
		if <-p.closeErr != nil {
			var m = 1
			for {
				err := p.Connect()
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

// 创建连接
func (p *Publisher) Connect() error {
	var err error
	p.conn, err = amqp.Dial(p.amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	// 后台协程检测异常关闭
	go func() {
		chanErr := <-p.conn.NotifyClose(make(chan *amqp.Error))
		log.Error(fmt.Sprintf("closing for: %s", chanErr))
		// 当 chanErr 为 nil 时，表示正常关闭；当!=nil 时，表示异常关闭
		if chanErr != nil {
			p.closeErr <- errors.New("Channel Closed")
		}
	}()

	log.Info("got Connection,getting Channel")
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	log.Info(fmt.Sprintf("got Channel,declaring Exchange (%q)", p.exchangeName))
	if err = p.channel.ExchangeDeclare(
		p.exchangeName,
		p.exchangeType,
		p.durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}
	return nil
}

func (p *Publisher) SendingToQueue(queueName, routingKey string, bodyType string, data []byte) error {
	p.queueName = queueName
	p.routingKey = routingKey

	//声明queue
	queue, err := p.channel.QueueDeclare(
		p.queueName, // queueName
		p.durable,   // durable
		false,       //autoDelete
		false,       // exclusive
		false,       //noWait
		nil,         //args
	)
	if err != nil {
		msg := "[MQ]_Failed to declare a queue"
		log.Error(msg)
		return err
	}

	// 通过 routingkey将queue和exchange绑定

	if err = p.channel.QueueBind(
		queue.Name,     // queue name
		p.routingKey,   // routing key
		p.exchangeName, // exchangeName
		false,          // noWait
		nil,            // args
	); err != nil {
		msg := "[MQ]_Failed to queue bind"
		log.Error(msg)
		return err
	}
	// 发送消息
	if bodyType == "json" {
		// 经测试发现，通过QueueBind绑定exchange之后，在Publish的时候就不能写exchangeName了，否则消息接收不到。设置成“”才行
		if err = p.channel.Publish(
			"",         // use the default exchangeName
			queue.Name, // routing key eg:our queue name
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType:  "application/json", //application/json
				Body:         data,
				DeliveryMode: 2, // 0，1: 瞬时  2：持久 Todo
			}); err != nil {
			msg := "[MQ]_Failed to publish a message"
			log.Error(msg)
			return err
		}
	} else {
		if err = p.channel.Publish(
			"",         // use the default exchangeName
			queue.Name, // routing key eg:our queue name
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType:  "text/plain", //text
				Body:         data,
				DeliveryMode: 2, // 0，1: 瞬时  2：持久
			}); err != nil {
			msg := "[MQ]_Failed to publish a message"
			log.Error(msg)
			return err
		}
	}
	return nil
}

// 断开连接
func (p *Publisher) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
