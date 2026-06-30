package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(url)
	if err != nil {
		fmt.Printf("error while creating a connection: %v\n", err)
	}
	fmt.Println("connection was successfully established")

	defer connection.Close()
	ch, err := connection.Channel()
	if err != nil {
		fmt.Printf("error while creating a channel: %v", err)
	}

	err = pubsub.PublishJSON(
		ch, 
		routing.ExchangePerilDirect, 
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("gracefully shutting down")
}
