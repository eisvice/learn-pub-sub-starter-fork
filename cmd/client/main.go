package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	url := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("error while creating a connection to RabbitMQ: %v\n", err)
	}
	fmt.Println("connection to RabbitMQ was successfully established")
	defer connection.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error while prompting for a username: %v\n", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)	
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("gracefully shutting down")
}
