// A sample kafka consumer that works with single or multi-part messages
// There are multiple ways to implement this, so use this as a reference only.
//
// @author Alejandro Galue <agalue@opennms.org>

package client

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/golang/protobuf/proto"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ProcessSinkMessage defines the action to execute after successfully receieved a Sink message
type ProcessSinkMessage func(msg []byte, wg *sync.WaitGroup)

// KafkaClient defines a simple Kafka consumer client
type KafkaClient struct {
	Bootstrap  string
	Topic      string
	GroupID    string
	Parameters string // CSV of KVP

	consumer     *kafka.Consumer
	msgBuffer    map[string][]byte
	chunkTracker map[string]int32
	waitGroup    *sync.WaitGroup
	mutex        *sync.Mutex
	stopping     bool
}

// Creates the Kafka Configuration Map
func (cli *KafkaClient) createConfig() *kafka.ConfigMap {
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

// Initializes all local variables
func (cli *KafkaClient) createVariables() {
	cli.msgBuffer = make(map[string][]byte)
	cli.chunkTracker = make(map[string]int32)
	cli.waitGroup = &sync.WaitGroup{}
	cli.mutex = &sync.Mutex{}
}

// Processes a Kafka message
// It return a non-empty slice when the message is complete
func (cli *KafkaClient) processMessage(msg *kafka.Message) []byte {
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
		cli.mutex.Lock()
		if cli.chunkTracker[id] < chunk {
			log.Printf("adding %d bytes to buffer for message %s", len(content), id)
			// Adds partial message to the buffer
			cli.msgBuffer[id] = append(cli.msgBuffer[id], content...)
			cli.chunkTracker[id] = chunk
		} else {
			log.Printf("chunk %d from %s was already processed, ignoring...\n", chunk, id)
		}
		cli.mutex.Unlock()
		return nil
	}
	// Retrieve the complete message from the buffer
	var data []byte
	if total == 1 { // Handle special case chunk == total == 1
		data = content
	} else {
		log.Printf("adding %d bytes to final message %s", len(content), id)
		cli.mutex.Lock()
		data = append(cli.msgBuffer[id], content...)
		cli.mutex.Unlock()
	}
	cli.bufferCleanup(id)
	return data
}

// Cleans up the chunk buffer. Should be called after successfully processed all chunks.
func (cli *KafkaClient) bufferCleanup(id string) {
	log.Printf("cleanup buffer for message %s", id)
	cli.mutex.Lock()
	if _, ok := cli.msgBuffer[id]; ok {
		delete(cli.msgBuffer, id)
	}
	if _, ok := cli.chunkTracker[id]; ok {
		delete(cli.chunkTracker, id)
	}
	cli.mutex.Unlock()
}

// Initialize builds the Kafka consumer object, and chunk buffers
func (cli *KafkaClient) Initialize() error {
	var err error
	cli.consumer, err = kafka.NewConsumer(cli.createConfig())
	if err != nil {
		return fmt.Errorf("cannot create consumer: %v", err)
	}
	err = cli.consumer.Subscribe(cli.Topic, nil)
	if err != nil {
		return fmt.Errorf("cannot subscribe to topic %s: %v", cli.Topic, err)
	}
	cli.createVariables()
	return nil
}

// Start reads messages from the given Kafka topic on an infinite loop
// It is recommended to use it within a Go Routine
func (cli *KafkaClient) Start(action ProcessSinkMessage) {
	cli.stopping = false
	for {
		if cli.stopping {
			return
		}
		msg, err := cli.consumer.ReadMessage(-1)
		if err == nil {
			if data := cli.processMessage(msg); data != nil {
				log.Printf("processing sink message of %d bytes", len(data))
				cli.waitGroup.Add(1)
				go action(data, cli.waitGroup)
			}
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("consumer error: %v (%v)", err, msg)
		}
	}
}

// Stop terminates the Kafka consumer and waits for the execution of all action handlers.
func (cli *KafkaClient) Stop() {
	cli.stopping = true
	cli.waitGroup.Wait()
	cli.consumer.Close()
}
