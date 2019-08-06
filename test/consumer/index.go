package main

import (
	"github.com/pefish/go-rabbitmq"
	"log"
)

func main() {


	forever := make(chan bool)

	go_rabbitmq.RabbitmqHelper.ConnectWithConfiguration(go_rabbitmq.Configuration{
		Host:     `localhost`,
		Username: `guest`,
		Password: `guest`,
	})
	c := go_rabbitmq.RabbitmqHelper.ConsumeDefault(`test`, func(data string) {
		log.Printf("Received a message: %s", data)
	})
	defer func() {
		c.Close()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
