package main

import (
	sr "github.com/chadit/MessageQueueExamples/rabbitmq/golang/service"
)

func main() {
	sr.Send()
	sr.Fetch()

}
