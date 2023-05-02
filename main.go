package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

// RideRequest represents a request for a ride.
type RideRequest struct {
	PassengerName string
	PickupAddress string
	Destination   string
}

// RideOffer represents an offer for a ride from a driver.
type RideOffer struct {
	DriverName string
	PickupETA  int
}

// RabbitMQ represents a RabbitMQ connection.
type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewRabbitMQ creates a new RabbitMQ connection.
func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &RabbitMQ{
		conn:    conn,
		channel: channel,
	}, nil
}

// PublishRideRequest publishes a ride request to RabbitMQ.
func (rmq *RabbitMQ) PublishRideRequest(request *RideRequest, exchange string) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	err = rmq.channel.Publish(
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	log.Printf("Ride request sent: %s", body)

	return nil
}

// ConsumeRideOffer consumes ride offers from RabbitMQ.
func (rmq *RabbitMQ) ConsumeRideOffer(queueName string, callback func(*RideOffer) error) error {
	msgs, err := rmq.channel.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			offer := &RideOffer{}
			err := json.Unmarshal(msg.Body, offer)
			if err != nil {
				log.Printf("Error parsing ride offer: %s", err)
			} else {
				err = callback(offer)
				if err != nil {
					log.Printf("Error processing ride offer: %s", err)
				}
			}
		}
	}()

	return nil
}

// PublishRideOffer publishes a ride offer to RabbitMQ.
func (rmq *RabbitMQ) PublishRideOffer(offer *RideOffer, exchange string) error {
	body, err := json.Marshal(offer)
	if err != nil {
		return err
	}

	err = rmq.channel.Publish(
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	log.Printf("Ride offer sent: %s", body)

	return nil
}

func main() {
	// Connect to RabbitMQ
	rmq, err := NewRabbitMQ("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	// Publish a ride request
	request := &RideRequest{
		PassengerName: "Alice",
		PickupAddress: "123 Main St",
		Destination:   "456 Oak St",
	}
	err = rmq.PublishRideRequest(request, "ride_requests")
	if err != nil {
		log.Fatalf("Failed to publish ride request: %s", err)
	}

	// Consume ride offers
	err = rmq.ConsumeRideOffer("ride_offers", func(offer *RideOffer) error {
		log.Printf("Received ride offer from %s with ETA %d", offer.DriverName, offer.PickupETA)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to consume ride offers: %s", err)
	}

	// Publish a ride offer
	offer := &RideOffer{
		DriverName: "Bob",
		PickupETA:  5,
	}
	err = rmq.PublishRideOffer(offer, "ride_offers")
	if err != nil {
		log.Fatalf("Failed to publish ride offer: %s", err)
	}

	// Wait for ride requests and offers to be processed
	forever := make(chan bool)
	log.Printf("Waiting for ride requests and offers...")
	<-forever
}
