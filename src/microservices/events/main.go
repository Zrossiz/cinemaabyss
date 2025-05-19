package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

var KAFKA_BROKERS = os.Getenv("KAFKA_BROKERS")
var Producer sarama.SyncProducer
var Consumer sarama.Consumer

func main() {
	producer, err := initProducer()
	if err != nil {
		panic(err)
	}
	Producer = producer
	defer Producer.Close()

	consumer, err := initConsumer()
	if err != nil {
		panic(err)
	}
	Consumer = consumer
	defer Consumer.Close()

	go startConsumer()

	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovie)
	http.HandleFunc("/api/events/user", handleUser)
	http.HandleFunc("/api/events/payment", handlePayment)

	http.ListenAndServe(":8082", nil)
}

func handleMovie(rw http.ResponseWriter, r *http.Request) {
	sendMessage("movie-events", "movie message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
}

func handleUser(rw http.ResponseWriter, r *http.Request) {
	sendMessage("user-events", "user message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
}

func handlePayment(rw http.ResponseWriter, r *http.Request) {
	sendMessage("payment-events", "payment message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
}

func handleHealth(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
}

func initProducer() (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(KAFKA_BROKERS, ","), cfg)
	if err != nil {
		return nil, fmt.Errorf("error init producer: %v", err)
	}

	return producer, nil
}

func initConsumer() (sarama.Consumer, error) {
	cfg := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(strings.Split(KAFKA_BROKERS, ","), cfg)
	if err != nil {
		return nil, fmt.Errorf("error init consumer: %v", err)
	}

	return consumer, nil
}

func startConsumer() {
	for _, topic := range [3]string{"movie-events", "user-events", "payment-events"} {
		partitions, _ := Consumer.Partitions(topic)
		for _, partition := range partitions {
			pc, _ := Consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)

			go func(pc sarama.PartitionConsumer, topic string) {
				for msg := range pc.Messages() {
					fmt.Printf("[Topic: %s] Message: %s\n", topic, string(msg.Value))
				}
			}(pc, topic)
		}
	}
}

func sendMessage(topic, payload string) {
	kafkaMessage := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(payload),
	}

	Producer.SendMessage(&kafkaMessage)
}
