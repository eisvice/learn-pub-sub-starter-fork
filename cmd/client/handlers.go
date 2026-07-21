package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix + "." + gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error while publishing a war declaration message")
				return pubsub.NackReque
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			fmt.Printf("Unknown outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		pubsub.PublishJSON(
			ch,
			routing.ExchangePerilTopic,
			"war",
			outcome,
		)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackReque
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeYouWon:
			err := pubsub.PublishGameLog(
				ch, 
				routing.GameLog{
					CurrentTime: time.Now(),
					Username: gs.GetUsername(),
					Message: fmt.Sprintf("%s won a wor against %s", winner, loser),
				},
			)
			if err != nil {
				fmt.Printf("error while publishing a game log %v\n", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeOpponentWon:
			err := pubsub.PublishGameLog(
				ch, 
				routing.GameLog{
					CurrentTime: time.Now(),
					Username: gs.GetUsername(),
					Message: fmt.Sprintf("%s won a wor against %s", winner, loser),
				},
			)
			if err != nil {
				fmt.Printf("error while publishing a game log %v\n", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := pubsub.PublishGameLog(
				ch, 
				routing.GameLog{
					CurrentTime: time.Now(),
					Username: gs.GetUsername(),
					Message: fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
				},
			)
			if err != nil {
				fmt.Printf("error while publishing a game log %v\n", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}