// A sample kafka consumer that works with single or multi-part messages
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/golang/protobuf/proto"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ProcessTelemetry the action to execute after successfully receieved a Cisco Telemetry message
type ProcessTelemetry func(msg *telemetry_bis.Telemetry)

// KafkaClient a simple Kafka consumer client
type KafkaClient struct {
	Bootstrap  string
	Topic      string
	GroupID    string
	Parameters string // CSV of KVP
	consumer   *kafka.Consumer
}

// Initialize build and initialize the Kafka consumer
func (cli *KafkaClient) Initialize() error {
	config := &kafka.ConfigMap{
		"bootstrap.servers": cli.Bootstrap,
		"group.id":          cli.GroupID,
		"auto.offset.reset": "latest",
	}
	if cli.Parameters != "" {
		for _, kv := range strings.Split(cli.Parameters, ",") {
			array := strings.Split(kv, "=")
			if err := config.SetKey(array[0], array[1]); err != nil {
				return fmt.Errorf("cannot add consumer config %s: %v", kv, err)
			}
		}
	}

	var err error
	cli.consumer, err = kafka.NewConsumer(config)
	if err != nil {
		return fmt.Errorf("cannot create consumer: %v", err)
	}

	cli.consumer.SubscribeTopics([]string{cli.Topic}, nil)
	return nil
}

// Start reads messages from the given Kafka topic on an infinite loop
// It is recommended to use it within a GoRoutine
func (cli *KafkaClient) Start(action ProcessTelemetry) {
	bufferMap := make(map[string][]byte)
	for {
		msg, err := cli.consumer.ReadMessage(-1)
		if err == nil {
			sinkMsg := &sink.SinkMessage{}
			if err := proto.Unmarshal(msg.Value, sinkMsg); err != nil {
				log.Printf("invalid message received: %v\n", err)
			} else {
				chunk := sinkMsg.GetCurrentChunkNumber() + 1 // Chunks starts at 0
				total := sinkMsg.GetTotalChunks()
				log.Printf("received message %s (chunk %d of %d, with %d bytes) on %s\n", sinkMsg.GetMessageId(), chunk, total, len(sinkMsg.GetContent()), msg.TopicPartition)
				if chunk != total {
					log.Printf("adding %d bytes to buffer %s", len(sinkMsg.GetContent()), sinkMsg.GetMessageId())
					// Adds partial message to the buffer
					bufferMap[sinkMsg.GetMessageId()] = append(bufferMap[sinkMsg.GetMessageId()], sinkMsg.GetContent()...)
				} else {
					// Retrieve the complete message from the buffer
					var data []byte
					if total == 1 { // Handle special case chunk == total == 1
						data = sinkMsg.GetContent()
					} else {
						data = append(bufferMap[sinkMsg.GetMessageId()], sinkMsg.GetContent()...)
					}
					// Process the message
					log.Printf("processing telemetry message of %d bytes\n", len(data))
					telemetry := &telemetry_bis.Telemetry{}
					err = proto.Unmarshal(data, telemetry)
					if err != nil {
						log.Printf("cannot parse message with telemetry_bis.proto: %v\n", err)
					} else {
						action(telemetry)
					}
					if _, ok := bufferMap[sinkMsg.GetMessageId()]; ok {
						delete(bufferMap, sinkMsg.GetMessageId())
					}
				}
			}
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("consumer error: %v (%v)\n", err, msg)
		}
	}
}

// Stop terminates the Kafka consumer
func (cli *KafkaClient) Stop() {
	cli.consumer.Close()
}

// The main function
func main() {
	client := KafkaClient{}
	flag.StringVar(&client.Bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&client.Topic, "topic", "telemetry-nxos", "kafka topic that will receive the messages")
	flag.StringVar(&client.GroupID, "group-id", "nxos-client", "the consumer group ID")
	flag.StringVar(&client.Parameters, "parameters", "", "optional kafka consumer parameters as a CSV of Key-Value pairs")
	flag.Parse()

	if err := client.Initialize(); err != nil {
		panic(err)
	}

	log.Printf("consumer started")
	go client.Start(func(msg *telemetry_bis.Telemetry) {
		// TODO Implement your custom actions here
		log.Printf("received message\n%s", proto.MarshalTextString(msg))
	})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Printf("stopping consumer")
	client.Stop()
	log.Printf("done!")
}
