package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/golang/protobuf/proto"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	var bootstrap, topic, groupID, parameters string
	flag.StringVar(&bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&topic, "topic", "telemetry-nxos", "kafka topic that will receive the messages")
	flag.StringVar(&groupID, "group-id", "nxos-client", "the consumer group ID")
	flag.StringVar(&parameters, "parameters", "", "optional kafka consumer parameters as a CSV of Key-Value pairs")
	flag.Parse()

	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"group.id":          groupID,
		"auto.offset.reset": "latest",
	}
	if parameters != "" {
		for _, kv := range strings.Split(parameters, ",") {
			array := strings.Split(kv, "=")
			if err := config.SetKey(array[0], array[1]); err != nil {
				log.Fatalf("cannot add consumer config %s: %v\n", kv, err)
			}
		}
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("cannot create consumer: %v\n", err)
	}

	consumer.SubscribeTopics([]string{topic}, nil)

	go func() {
		var buffer bytes.Buffer
		for {
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				sinkMsg := &sink.SinkMessage{}
				if err := proto.Unmarshal(msg.Value, sinkMsg); err != nil {
					log.Printf("invalid message received: %v\n", err)
				} else {
					chunk := sinkMsg.GetCurrentChunkNumber() + 1 // Chunks starts at 0
					total := sinkMsg.GetTotalChunks()
					log.Printf("message %s (chunk %d of %d) received on %s\n", sinkMsg.GetMessageId(), chunk, total, msg.TopicPartition)
					if chunk != total {
						// Adds partial message to the buffer
						buffer.Write(sinkMsg.GetContent())
					} else {
						// Retrieve the complete message from the buffer
						var data = make([]byte, buffer.Len())
						if total == 1 { // Handle special case chunk == total == 1
							data = sinkMsg.GetContent()
						} else {
							_, err := buffer.Read(data)
							if err != nil {
								log.Printf("cannot retrieve data from buffer: %v\n", err)
							}
						}
						// Process the message
						log.Printf("processing %d bytes from buffer\n", len(data))
						telemetry := &telemetry_bis.Telemetry{}
						err = proto.Unmarshal(data, telemetry)
						if err != nil {
							log.Printf("cannot parse message with telemetry_bis.proto: %v\n", err)
						} else {
							log.Printf("received message\n,%s", proto.MarshalTextString(telemetry))
							// TODO Add your logic here
						}
						buffer.Reset()
					}
				}
			} else {
				// The client will automatically try to recover from all errors.
				log.Printf("consumer error: %v (%v)\n", err, msg)
			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	consumer.Close()
}
