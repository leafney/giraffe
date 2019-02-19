package publisher

import (
	"encoding/json"
	"github.com/Leafney/giraffe/mqs"
	"testing"
	"time"
)

var p *Publisher

func TestP(t *testing.T) {


	go func() {
		time.Sleep(3*time.Second)

		Send()
		t.Log("发送了任务1")
	}()

	go func() {
		time.Sleep(2*time.Second)

		Send()
		t.Log("发送了任务2")
	}()

	// 退出时关闭MQ连接
	defer mqs.CloseConnect()


	for {
		mqs.InitConnect(
			"",
			"",
			"",
			"direct",
			true,
			true,
			)
		t.Log("MQ连接异常，10分钟后将自动重连...")
		time.Sleep(10 * time.Minute) // 暂停10分钟后再重试
	}

}

func Send()  {
	h := Hello{Code: 1, Msg: "hello world222"}
	body, _ := json.Marshal(h)
	mqs.MQSendJsonTask([]byte(body),"test","test")
}

type Hello struct {
	Code int
	Msg  string
}