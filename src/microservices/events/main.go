package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var KAFKA_BROKERS = os.Getenv("KAFKA_BROKERS")
var SERVER_PORT = os.Getenv("PORT")
var Producer sarama.SyncProducer
var Consumer sarama.Consumer

func main() {
	time.Sleep(5 * time.Second)

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

	http.ListenAndServe(SERVER_PORT, nil)
}

func handleMovie(rw http.ResponseWriter, r *http.Request) {
	sendMessage("movie-events", "movie message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusCreated)
	json.NewEncoder(rw).Encode(map[string]string{"status": "success"})
}

func handleUser(rw http.ResponseWriter, r *http.Request) {
	sendMessage("user-events", "user message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusCreated)
	json.NewEncoder(rw).Encode(map[string]string{"status": "success"})
}

func handlePayment(rw http.ResponseWriter, r *http.Request) {
	sendMessage("payment-events", "payment message")
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusCreated)
	json.NewEncoder(rw).Encode(map[string]string{"status": "success"})
}

func handleHealth(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	json.NewEncoder(rw).Encode(map[string]bool{"status": true})
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
		partitions, err := Consumer.Partitions(topic)
		if err != nil {
			fmt.Printf("Error fetching partitions for topic %s: %v\n", topic, err)
			continue
		}

		for _, partition := range partitions {
			pc, err := Consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				fmt.Printf("Error consuming partition %d of topic %s: %v\n", partition, topic, err)
				continue
			}

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
