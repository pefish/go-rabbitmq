package p_rabbitmq

import (
	"fmt"
	"github.com/pefish/go-application"
	"github.com/pefish/go-logger"
	"github.com/streadway/amqp"
	"time"
)

type RabbitmqClass struct {
	Conn *amqp.Connection
}

var Rabbitmq = RabbitmqClass{}

func (this *RabbitmqClass) Close() {
	if this.Conn != nil {
		this.Conn.Close()
	}
}

func (this *RabbitmqClass) Connect(username string, password string, host string, port string, vhost string) {
	conn, err := amqp.Dial(fmt.Sprintf(`amqp://%s:%s@%s:%s/%s`, username, password, host, port, vhost))
	if err != nil {
		panic(err)
	}
	this.Conn = conn
}

func (this *RabbitmqClass) PublishDefault(quene string, data string) {
	if p_application.Application.Debug {
		p_logger.Logger.Debug(quene, data)
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
