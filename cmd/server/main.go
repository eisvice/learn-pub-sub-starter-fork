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

	err = pubsub.SubscribeGob(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.Durable,
		handlerWriteLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to game log: %v", err)	
	}

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

func handlerWriteLog() func(routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)	
		if err != nil {
			fmt.Printf("error while writing a log to disk in handlerWriteLog: %v\n", err)
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}