package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int 

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	message, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("error while marshaling JSON")
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body: message,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, 
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == Durable, // durable
		queueType == Transient, // autoDelete
		queueType == Transient, // exclusive
		false, // noWait
		nil, // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error while declaring a queue: %v", err)
	}

	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error while binding a queue: %v", err)
	}
	
	return ch, queue, nil
}

func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("can't bind the queue %v to the exchange %v: %v\n", queueName, exchange, err)
	}

	ampqChan, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error while consuming the message: %v\n", err)
	}

	var result T
	go func() {
		for msg := range ampqChan {
			err = json.Unmarshal(msg.Body, &result)
			if err != nil {
				fmt.Printf("could not unmarshal the message: %v\n", err)
			}
	
			handler(result)
			err = msg.Ack(false)
			if err != nil {
				log.Fatalf("errors during acknowledgement: %v\n", err)
			}
		}
	}()

	return nil
}