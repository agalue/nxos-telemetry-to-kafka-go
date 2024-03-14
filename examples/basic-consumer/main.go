// A sample kafka consumer that works with single or multi-part messages
//
// @author Alejandro Galue <agalue@opennms.org>

package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/agalue/onms-kafka-ipc-receiver/client"
	"google.golang.org/protobuf/proto"
)

// The main function
func main() {
	cli := client.KafkaClient{IPC: "sink"}
	flag.StringVar(&cli.Bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&cli.Topic, "topic", "OpenNMS.Sink.Telemetry-NXOS", "kafka topic that will receive the messages")
	flag.StringVar(&cli.GroupID, "group-id", "nxos-client", "the consumer group ID")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("starting consumer")
	if err := cli.Initialize(ctx); err != nil {
		panic(err)
	}
	log.Println("consumer started")

	go cli.Start(func(data []byte) {
		/////////////////////////////////////////////
		// TODO Implement your custom actions here //
		/////////////////////////////////////////////
		telemetry := &telemetry_bis.Telemetry{}
		if err := proto.Unmarshal(data, telemetry); err == nil {
			log.Printf("received telemetry message\n%s", telemetry.String())
		} else {
			log.Printf("cannot parse message with telemetry_bis.proto: %v\nmessage: %v", err, data)
		}
	})

	<-ctx.Done()
	log.Println("done!")
}
