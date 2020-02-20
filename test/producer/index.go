package main

import (
	"github.com/pefish/go-logger"
	"github.com/pefish/go-rabbitmq"
	"strconv"
	"time"
)

func main() {
	go_logger.Logger.Init(`test`, ``)
	go_rabbitmq.RabbitmqHelper.SetLogger(go_logger.Logger).MustConnectWithConfiguration(go_rabbitmq.Configuration{
		Host: `localhost`,
		Username: `guest`,
		Password: `guest`,
	})
	i := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		i++
		if i >= 10 {
			break
		}
		go_rabbitmq.RabbitmqHelper.MustPublishDefault(`test_queue`, strconv.FormatInt(int64(i), 10))
	}
}

