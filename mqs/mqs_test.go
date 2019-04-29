package mqs

import (
	"testing"
	"time"
)

// go test -v -run TestP -timeout 30m
func TestM(t *testing.T) {

	go func() {
		time.Sleep(3 * time.Second)

		Send()
		t.Log("发送了任务1")
	}()

	go func() {
		time.Sleep(2 * time.Second)

		Send()
		t.Log("发送了任务2")
	}()

	// 退出时关闭MQ连接
	defer CloseConnect()

	for {
		InitConnect("",
			"",
			"",
			"direct",
			true,
		)
		t.Log("MQ连接异常，10分钟后将自动重连...")
		time.Sleep(10 * time.Minute) // 暂停10分钟后再重试
	}
}

func Send() {
	MQSendTextTask("Hello World", "test", "test")
}
