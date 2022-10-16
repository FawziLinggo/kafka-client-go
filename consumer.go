package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Print(os.Stderr, "Usege : %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}

	FileConfig := os.Args[1]
	conf := ReadConfig(FileConfig)

	consumer, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create Consumer : %s", err)
		os.Exit(1)
	}

	topic := "purchases"
	err = consumer.SubscribeTopics([]string{topic}, nil)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	running := true

	for running == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v : Terminating\n", sig)
			running = false
		default:
			ev, err := consumer.ReadMessage(100 * time.Microsecond)
			if err != nil {
				continue
			} else {
				fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
					*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
			}
		}

	}
	consumer.Close()
}
