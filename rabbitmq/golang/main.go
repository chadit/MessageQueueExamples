package main

import (
	"log"

	sr "github.com/chadit/MessageQueueExamples/rabbitmq/golang/service"
)

func main() {
	forever := make(chan bool)
	sr.Send()
	go sr.Fetch()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
