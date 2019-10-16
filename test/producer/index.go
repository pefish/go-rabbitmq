package main

import (
	"fmt"
	"os"
	"time"
)

func main() {

	//go_rabbitmq.RabbitmqHelper.MustConnectWithConfiguration(go_rabbitmq.Configuration{
	//	Host: `localhost`,
	//	Username: `guest`,
	//	Password: `guest`,
	//})
	//for i := 0; i < 1000; i++ {
	//	time.Sleep(3 * time.Second)
	//	go_rabbitmq.RabbitmqHelper.MustPublishDefault(`test1`, strconv.FormatInt(int64(i), 10))
	//}
	go test()
	for {
		time.Sleep(2 * time.Second)
		fmt.Println(11)
	}
}

func test() error {
	os.Exit(1)
	return nil
}
