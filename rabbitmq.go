package go_rabbitmq

import (
	"fmt"
	"github.com/pefish/go-logger"
	"github.com/streadway/amqp"
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

	var vhost = ``
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
	go_logger.Logger.Info(fmt.Sprintf(`rabbitmq connect succeed. url: %s:%d`, host, port))
}

func (this *RabbitmqClass) ConsumeDefault(quene string, doFunc func(data string)) *amqp.Channel {
	c, err := this.Conn.Channel()
	if err != nil {
		panic(err)
	}

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
		false,  // no-local 不支持的参数
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(err)
	}

	go func() {
		for d := range msgsChan {
			go_logger.Logger.Info(fmt.Sprintf(`rabbitmq consume; quene: %s, body: %s`, quene, string(d.Body)))
			doFunc(string(d.Body))
		}
	}()
	go_logger.Logger.Info(fmt.Sprintf(`rabbitmq subscribe succeed. quene: %s`, quene))
	return c
}

func (this *RabbitmqClass) NewChannel() *amqp.Channel {
	c, err := this.Conn.Channel()
	if err != nil {
		panic(err)
	}
	return c
}

//func (this *RabbitmqClass) DeclareQueneWithDeadLetter(c amqp.Channel) {
//	q, err := c.QueueDeclare(`dead_letter_quene`, true, false, false, false, amqp.Table{
//		`x-dead-letter-exchange`: `dead_letter_exchange`,
//		`x-dead-letter-routing-key`: `dead_letter_routing_key`,
//		`x-message-ttl`: int32(5000),
//	})
//	if err != nil {
//		panic(err)
//	}
//}

func (this *RabbitmqClass) DeclareDeadLetterQuene(c amqp.Channel) (string, string) {
	exchangeName := `dead_letter_exchange`
	queneName := `dead_letter_quene`
	err := c.ExchangeDeclare(exchangeName, `direct`, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	deadLetterQ, err := c.QueueDeclare(queneName, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	err = c.QueueBind(deadLetterQ.Name, `dead_letter_routing_key`, exchangeName, false, nil)
	if err != nil {
		panic(err)
	}
	return exchangeName, queneName
}

func (this *RabbitmqClass) PublishDefault(quene string, data string) {
	go_logger.Logger.Info(fmt.Sprintf(`rabbitmq publish; quene: %s, body: %s`, quene, data))

	c := this.NewChannel()
	defer c.Close()

	q, err := c.QueueDeclare(
		quene, // name
		true,  // durable 队列持久化
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent, // 消息持久化
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(data),
	}
	err = c.Publish("", q.Name, false, false, msg)
	if err != nil {
		panic(err)
	}
}
