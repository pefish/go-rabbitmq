package main

import (
	"github.com/pefish/go-rabbitmq"
	"log"
)

func main() {
	go_rabbitmq.RabbitmqHelper.ConnectWithConfiguration(go_rabbitmq.Configuration{
		Host:     `localhost`,
		Username: `guest`,
		Password: `guest`,
	})
	go_rabbitmq.RabbitmqHelper.ConsumeDefault(`test`, func(data string) {
		log.Printf("Received a message: %s", data)
	})


}
