package main

import (
	"fmt"
	"log"
	"math/rand"

	gen "github.com/brianvoe/gofakeit/v5"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var count=100000

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for i := 1; i <= count; i++ {
		//body := "Hello World!"
		data, err := generateData()
		failOnError(err, "Failed to generate data")

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        data,
			})
		//log.Printf(" [x] Sent %s", data)
		failOnError(err, "Failed to publish a message")
	}
}

func generateData() ([]byte, error)  {
	gen.Seed(rand.Int63())

	var value, err = gen.JSON(&gen.JSONOptions{
		Type: "object",
		Fields: []gen.Field{
			{Name: "first_name", Function: "firstname"},
			{Name: "last_name", Function: "lastname"},
			{Name: "address", Function: "address"},
			{Name: "password", Function: "password", Params: map[string][]string{"special": {"false"}}},
		},
		Indent: true,
	})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return value, nil
}
