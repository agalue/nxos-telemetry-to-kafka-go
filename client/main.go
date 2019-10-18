// A sample kafka consumer that works with single or multi-part messages
// There are multiple ways to implement this, so use this as a reference only.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	consumer     *kafka.Consumer
	msgBuffer    map[string][]byte
	chunkTracker map[string]int32
}

// CreateConfig creates the Kafka Configuration Map
func (cli *KafkaClient) CreateConfig() *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers":     cli.Bootstrap,
		"group.id":              cli.GroupID,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "latest",
		"broker.address.family": "v4",
	}
	if cli.Parameters != "" {
		for _, kv := range strings.Split(cli.Parameters, ",") {
			array := strings.Split(kv, "=")
			if err := config.SetKey(array[0], array[1]); err != nil {
				fmt.Printf("cannot add consumer config %s: %v", kv, err)
			}
		}
	}
	return config
}

// Initialize build and initialize the Kafka consumer
func (cli *KafkaClient) Initialize() error {
	var err error
	cli.consumer, err = kafka.NewConsumer(cli.CreateConfig())
	if err != nil {
		return fmt.Errorf("cannot create consumer: %v", err)
	}
	err = cli.consumer.Subscribe(cli.Topic, nil)
	if err != nil {
		return fmt.Errorf("cannot subscribe to topic %s: %v", cli.Topic, err)
	}
	cli.msgBuffer = make(map[string][]byte)
	cli.chunkTracker = make(map[string]int32)
	return nil
}

// ProcessMessage processes a Kafka message
// It return a non-empty slice when the message is complete
func (cli *KafkaClient) ProcessMessage(msg *kafka.Message) []byte {
	sinkMsg := &sink.SinkMessage{}
	if err := proto.Unmarshal(msg.Value, sinkMsg); err != nil {
		log.Printf("invalid message received: %v\n", err)
		return nil
	}
	chunk := sinkMsg.GetCurrentChunkNumber() + 1 // Chunks starts at 0
	total := sinkMsg.GetTotalChunks()
	id := sinkMsg.GetMessageId()
	content := sinkMsg.GetContent()
	log.Printf("received message %s (chunk %d of %d, with %d bytes) on %s\n", id, chunk, total, len(content), msg.TopicPartition)
	if chunk != total {
		if cli.chunkTracker[id] < chunk {
			log.Printf("adding %d bytes to buffer for message %s", len(content), id)
			// Adds partial message to the buffer
			cli.msgBuffer[id] = append(cli.msgBuffer[id], content...)
			cli.chunkTracker[id] = chunk
		} else {
			log.Printf("chunk %d from %s was already processed, ignoring...\n", chunk, id)
		}
		return nil
	}
	// Retrieve the complete message from the buffer
	var data []byte
	if total == 1 { // Handle special case chunk == total == 1
		data = content
	} else {
		log.Printf("adding %d bytes to final message %s", len(content), id)
		data = append(cli.msgBuffer[id], content...)
	}
	cli.bufferCleanup(id)
	return data
}

func (cli *KafkaClient) bufferCleanup(id string) {
	log.Printf("cleanup buffer for message %s\n", id)
	if _, ok := cli.msgBuffer[id]; ok {
		delete(cli.msgBuffer, id)
	}
	if _, ok := cli.chunkTracker[id]; ok {
		delete(cli.chunkTracker, id)
	}
}

func (cli *KafkaClient) getTelemetry(data []byte) *telemetry_bis.Telemetry {
	telemetry := &telemetry_bis.Telemetry{}
	if err := proto.Unmarshal(data, telemetry); err != nil {
		log.Printf("cannot parse message with telemetry_bis.proto: %v\n", err)
		return nil
	}
	return telemetry
}

// Start reads messages from the given Kafka topic on an infinite loop
// It is recommended to use it within a GoRoutine
func (cli *KafkaClient) Start(action ProcessTelemetry) {
	for {
		msg, err := cli.consumer.ReadMessage(-1)
		if err == nil {
			if data := cli.ProcessMessage(msg); data != nil {
				log.Printf("processing telemetry message of %d bytes\n", len(data))
				if telemetry := cli.getTelemetry(data); telemetry != nil {
					go action(telemetry)
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
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("stopping consumer")
	client.Stop()
	log.Printf("done!")
}
