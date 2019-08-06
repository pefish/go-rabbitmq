package main

import (
	"github.com/pefish/go-rabbitmq"
	"strconv"
)

func main() {
	go_rabbitmq.RabbitmqHelper.ConnectWithConfiguration(go_rabbitmq.Configuration{
		Host: `localhost`,
		Username: `guest`,
		Password: `guest`,
	})
	for i := 0; i < 500; i++ {
		go_rabbitmq.RabbitmqHelper.PublishDefault(`test`, strconv.FormatInt(int64(i), 10))
	}
}
