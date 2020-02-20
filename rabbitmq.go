package go_rabbitmq

import (
	"fmt"
	"github.com/pefish/go-logger"
	"github.com/streadway/amqp"
	"time"
)

type RabbitmqClass struct {
	Conn   *amqp.Connection
	logger go_logger.InterfaceLogger
}

var RabbitmqHelper = RabbitmqClass{}

func (this *RabbitmqClass) SetLogger(logger go_logger.InterfaceLogger) *RabbitmqClass {
	this.logger = logger
	return this
}

func (this *RabbitmqClass) Close() {
	if this.Conn != nil {
		this.Conn.Close()
	}
}

func (this *RabbitmqClass) ConnectWithMap(map_ map[string]interface{}) error {
	var port uint64 = 5672
	if map_[`port`] != nil {
		port = uint64(map_[`port`].(float64))
	}

	var vhost = ``
	if map_[`vhost`] != nil {
		vhost = map_[`vhost`].(string)
	}
	err := this.Connect(map_[`username`].(string), map_[`password`].(string), map_[`host`].(string), port, vhost)
	if err != nil {
		return err
	}
	return nil
}

func (this *RabbitmqClass) MustConnectWithMap(map_ map[string]interface{}) {
	err := this.ConnectWithMap(map_)
	if err != nil {
		panic(err)
	}
}

type Configuration struct {
	Host     string
	Port     uint64
	Username string
	Password string
	Vhost    string
}

func (this *RabbitmqClass) ConnectWithConfiguration(configuration Configuration) error {
	var port uint64 = 5672
	if configuration.Port != 0 {
		port = configuration.Port
	}
	err := this.Connect(configuration.Username, configuration.Password, configuration.Host, port, configuration.Vhost)
	if err != nil {
		return err
	}
	return nil
}

func (this *RabbitmqClass) MustConnectWithConfiguration(configuration Configuration) {
	err := this.ConnectWithConfiguration(configuration)
	if err != nil {
		panic(err)
	}
}

func (this *RabbitmqClass) Connect(username string, password string, host string, port uint64, vhost string) error {
	url := fmt.Sprintf(`amqp://%s:%s@%s:%d/%s`, username, password, host, port, vhost)
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	this.Conn = conn
	this.logger.Info(fmt.Sprintf(`rabbitmq connect succeed. url: %s:%d`, host, port))
	return nil
}

func (this *RabbitmqClass) MustConnect(username string, password string, host string, port uint64, vhost string) {
	err := this.Connect(username, password, host, port, vhost)
	if err != nil {
		panic(err)
	}
}

func (this *RabbitmqClass) MustConsumeDefault(quene string, doFunc func(data string)) *amqp.Channel {
	c, err := this.ConsumeDefault(quene, doFunc)
	if err != nil {
		panic(err)
	}
	return c
}

func (this *RabbitmqClass) ConsumeDefault(quene string, doFunc func(data string)) (*amqp.Channel, error) {
	c, err := this.NewChannel()
	if err != nil {
		return nil, err
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
		return nil, err
	}

	msgsChan, err := c.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local 不支持的参数
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	go func() {
		tempFun := func(d amqp.Delivery) {
			defer func() {
				if err := recover(); err != nil {
					this.logger.Error(err)
					err := d.Reject(false)
					if err != nil {
						panic(err)
					}
				} else {
					err := d.Ack(false)
					if err != nil {
						panic(err)
					}
				}
			}()
			this.logger.DebugF(`rabbitmq consume; quene: %s, body: %s`, quene, string(d.Body))
			doFunc(string(d.Body))
		}

		for d := range msgsChan {
			tempFun(d)
		}
	}()
	this.logger.InfoF(`rabbitmq subscribe succeed. quene: %s`, quene)
	return c, nil
}

func (this *RabbitmqClass) NewChannel() (*amqp.Channel, error) {
	c, err := this.Conn.Channel()
	if err != nil {
		return nil, err
	}
	err = c.Qos(1, 0, true) // 此通道上的消息只能一个个被消费（多个消费者的情况下）。前一个消息没有ack，后一个消息等待. auto-ack为false才生效
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (this *RabbitmqClass) MustNewChannel() *amqp.Channel {
	c, err := this.NewChannel()
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

func (this *RabbitmqClass) DeclareDeadLetterQuene(c amqp.Channel) (string, string, error) {
	exchangeName := `dead_letter_exchange`
	queneName := `dead_letter_quene`
	err := c.ExchangeDeclare(exchangeName, `direct`, true, false, false, false, nil)
	if err != nil {
		return ``, ``, err
	}
	deadLetterQ, err := c.QueueDeclare(queneName, true, false, false, false, nil)
	if err != nil {
		return ``, ``, err
	}
	err = c.QueueBind(deadLetterQ.Name, `dead_letter_routing_key`, exchangeName, false, nil)
	if err != nil {
		return ``, ``, err
	}
	return exchangeName, queneName, nil
}

func (this *RabbitmqClass) MustDeclareDeadLetterQuene(c amqp.Channel) (string, string) {
	exchangeName, queneName, err := this.DeclareDeadLetterQuene(c)
	if err != nil {
		panic(err)
	}
	return exchangeName, queneName
}

func (this *RabbitmqClass) PublishDefault(quene string, data string) error {
	this.logger.DebugF(`rabbitmq publish; quene: %s, body: %s`, quene, data)

	c, err := this.NewChannel()
	if err != nil {
		return err
	}
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
		return err
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent, // 消息持久化
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(data),
	}
	err = c.Publish("", q.Name, false, false, msg)
	if err != nil {
		return err
	}
	return nil
}

func (this *RabbitmqClass) MustPublishDefault(quene string, data string) {
	err := this.PublishDefault(quene, data)
	if err != nil {
		panic(err)
	}
}
