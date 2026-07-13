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
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("error while creating a connection: %v\n", err)
	}
	defer connection.Close()
	fmt.Println("Peril game server connected to RabbitMQ")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error while creating a channel: %v\n", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.Durable,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)	
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	OuterLoop:
		for {
			clientsInputs := gamelogic.GetInput()
			if len(clientsInputs) == 0 {
				continue
			}

			switch clientsInputs[0] {
			case "pause":
				err = pubsub.PublishJSON(
					ch, 
					routing.ExchangePerilDirect, 
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: true,
					},
				)
				if err != nil {
					log.Printf("could not publish time: %v", err)
				}
			case "resume":
				err = pubsub.PublishJSON(
					ch, 
					routing.ExchangePerilDirect, 
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: false,
					},
				)
				if err != nil {
					log.Printf("could not publish time: %v", err)
				}
			case "quit":
				fmt.Println("finishing session...")
				break OuterLoop
			default:
				fmt.Println("i don't understand the command " + clientsInputs[0])
			}
		}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("gracefully shutting down")
}
