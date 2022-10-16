package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage : %s /config.properties\n", os.Args[0])
		os.Exit(1)
	}
	configFile := os.Args[1]
	conf := ReadConfig(configFile)
	topic := "purchases"

	producer, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("failed to create producer : %s", err)
		os.Exit(1)
	}

	go func() {
		for event := range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message : %v\n ", ev.TopicPartition)
				} else {
					fmt.Printf("Produce event to  topic %s : key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := [...]string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 10; n++ {
		key := users[rand.Intn(len(users))]
		value := items[rand.Intn(len(items))]

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key),
			Value: []byte(value),
		}, nil)
	}

	producer.Flush(15 * 1000)
	producer.Close()

}
