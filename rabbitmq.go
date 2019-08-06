package go_rabbitmq

import (
	"fmt"
	"github.com/pefish/go-application"
	"github.com/pefish/go-logger"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type RabbitmqClass struct {
	Conn *amqp.Connection
}

var RabbitmqHelper = RabbitmqClass{}

func (this *RabbitmqClass) Close() {
	if this.Conn != nil {
		this.Conn.Close()
	}
}

func (this *RabbitmqClass) ConnectWithMap(map_ map[string]interface{}) {
	var port uint64 = 5672
	if map_[`port`] != nil {
		port = map_[`port`].(uint64)
	}

	var vhost string = ``
	if map_[`vhost`] != nil {
		vhost = map_[`vhost`].(string)
	}
	this.Connect(map_[`username`].(string), map_[`password`].(string), map_[`host`].(string), port, vhost)
}

type Configuration struct {
	Host     string
	Port     uint64
	Username string
	Password string
	Vhost    string
}

func (this *RabbitmqClass) ConnectWithConfiguration(configuration Configuration) {
	var port uint64 = 5672
	if configuration.Port != 0 {
		port = configuration.Port
	}
	this.Connect(configuration.Username, configuration.Password, configuration.Host, port, configuration.Vhost)
}

func (this *RabbitmqClass) Connect(username string, password string, host string, port uint64, vhost string) {
	url := fmt.Sprintf(`amqp://%s:%s@%s:%d/%s`, username, password, host, port, vhost)
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}
	this.Conn = conn
	go_logger.Logger.Info(fmt.Sprintf(`rabbitmq connect succeed. url: %s`, url))
}

func (this *RabbitmqClass) ConsumeDefault(quene string, doFunc func(data string)) {
	c, err := this.Conn.Channel()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	q, err := c.QueueDeclare(
		quene, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}

	msgsChan, err := c.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msgsChan {
			if go_application.Application.Debug {
				go_logger.Logger.Debug(quene, string(d.Body))
			}
			doFunc(string(d.Body))
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (this *RabbitmqClass) PublishDefault(quene string, data string) {
	if go_application.Application.Debug {
		go_logger.Logger.Debug(quene, data)
	}

	c, err := this.Conn.Channel()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	q, err := c.QueueDeclare(
		quene, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(data),
	}
	err = c.Publish("", q.Name, false, false, msg)
	if err != nil {
		panic(err)
	}
}
