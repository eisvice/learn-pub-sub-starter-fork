package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int 

const (
	Durable SimpleQueueType = iota
	Transient
)

type Acktype int
const (
	Ack Acktype = iota
	NackReque 
	NackDiscard
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
		amqp.Table{
			"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
		}, // args
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
    handler func(T) Acktype,
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
	
			ackType := handler(result)
			switch ackType {
			case Ack:
				err = msg.Ack(false)
			case NackReque:
				err = msg.Nack(false, true)
			case NackDiscard:
				err = msg.Nack(false, false)
			}
			if err != nil {
				log.Fatalf("errors during acknowledgement %v: %v\n", ackType, err)
			}
		}
	}()

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return fmt.Errorf("error while encoding value to gob")
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body: buf.Bytes(),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, gl routing.GameLog) error {
	err := PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug + "." + gl.Username,
		gl,
	)
	if err != nil {
		return err
	}

	return nil
}