// A sample kafka consumer that works with single or multi-part messages
// There are multiple ways to implement this, so use this as a reference only.
//
// @author Alejandro Galue <agalue@opennms.org>

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/agalue/nxos-telemetry-to-kafka-go/examples/basic-consumer/client"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/golang/protobuf/proto"
)

// The main function
func main() {
	cli := client.KafkaClient{}
	flag.StringVar(&cli.Bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&cli.Topic, "topic", "OpenNMS.Sink.Telemetry-NXOS", "kafka topic that will receive the messages")
	flag.StringVar(&cli.GroupID, "group-id", "nxos-client", "the consumer group ID")
	flag.StringVar(&cli.Parameters, "parameters", "", "optional kafka consumer parameters as a CSV of Key-Value pairs")
	flag.Parse()

	log.Println("starting consumer")
	if err := cli.Initialize(); err != nil {
		panic(err)
	}
	log.Println("consumer started")

	go cli.Start(func(data []byte, wg *sync.WaitGroup) {
		defer wg.Done() // Mandatory line

		/////////////////////////////////////////////
		// TODO Implement your custom actions here //
		/////////////////////////////////////////////

		telemetry := &telemetry_bis.Telemetry{}
		if err := proto.Unmarshal(data, telemetry); err != nil {
			log.Printf("received telemetry message\n%s", proto.MarshalTextString(telemetry))
		} else {
			log.Printf("cannot parse message with telemetry_bis.proto: %v\nmessage: %v", err, data)
		}
	})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("stopping consumer")
	cli.Stop()
	log.Println("done!")
}
