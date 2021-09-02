package main

import (
	"log"
	"runtime"

	amqp "github.com/rabbitmq/amqp091-go"
)

func errorLoger(err error, msg string) {

	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
func main() {

	runtime.GOMAXPROCS(2)

	conn, err := amqp.Dial("amqp://user:user@172.31.201.78:5672")
	errorLoger(err, "Filed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	errorLoger(err, "Failed to open a channel")
	defer ch.Close()

	queues, err := ch.QueueDeclare(
		"hello", //name
		true,    //durable
		false,   //delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     //arguments
	)
	errorLoger(err, "Failed to declare a queue")
	msgs, err := ch.Consume(
		queues.Name,    // очередь
		"OUT ConSumer", //консумер
		true,           //авто -акк
		false,          //эксклюзив
		false,          //не локально
		false,          // не ждать
		nil,            //args

	)
	errorLoger(err, "Failed to register a consumer")
	forever := make(chan bool)
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
	}
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
