package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"

	gen "github.com/brianvoe/gofakeit/v5"
	flags "github.com/jessevdk/go-flags"
	"github.com/streadway/amqp"
)

var opts struct {
	Count    int    `short:"c" long:"count" env:"COUNT" default:"100" description:"count of records"`
	Queue    string `short:"q" long:"queue" env:"QUEUE" default:"hello" description:"name of queue"`
	Hostname string `short:"h" long:"host" env:"HOSTNAME" default:"localhost" description:"hostname of RabbitMQ server"`
	Port     int    `short:"p" long:"port" env:"PORT" default:"5672" description:"port of RabbitMQ server"`
	Username string `long:"username" env:"USER" default:"guest" description:"username"`
	Password string `long:"password" env:"PASS" default:"guest" description:"password"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	conn, err := amqp.Dial("amqp://"+opts.Username+":"+opts.Password+"@"+opts.Hostname+":"+strconv.Itoa(opts.Port)+"/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		opts.Queue, // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for i := 1; i <= opts.Count; i++ {
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
	log.Printf("%d messages sent\n", opts.Count)

}

func generateData() ([]byte, error) {
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
