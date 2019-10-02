package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	kafkaServer := flag.String("bootstrap", "localhost:9092", "kafka bootstrap server")
	kafkaTopic := flag.String("topic", "telemetry-nxos", "kafka topic that will receive the messages")
	port := flag.Int("port", 50001, "port to listen for gRPC requests")
	flag.Parse()

	log.Printf("starting gRPC server on port %d\n", *port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("could not listen to port %d: %v", *port, err)
	}

	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *kafkaServer})
	if err != nil {
		log.Fatalf("could not create producer: %v", err)
	}

	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	grpcServer := grpc.NewServer()
	mdt_dialout.RegisterGRPCMdtDialoutServer(grpcServer, dialoutServer{kafkaProducer, *kafkaTopic})

	go func() {
		err = grpcServer.Serve(listener)
		if err != nil {
			log.Fatalf("could not serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Println("shutting down...")
	grpcServer.GracefulStop()
	kafkaProducer.Close()
	log.Println("done!")
}

type dialoutServer struct {
	producer *kafka.Producer
	topic    string
}

func (srv dialoutServer) MdtDialout(stream mdt_dialout.GRPCMdtDialout_MdtDialoutServer) error {
	peer, peerOK := peer.FromContext(stream.Context())
	if peerOK {
		log.Printf("accepted Cisco MDT GRPC dialout connection from %s\n", peer.Addr)
	}
	for {
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("session closed")
			} else {
				log.Println("session error")
			}
			return err
		}

		if len(reply.Data) == 0 && len(reply.Errors) != 0 {
			log.Printf("error from client %s, %s\n", peer.Addr, reply.Errors)
			return nil
		}

		log.Printf("received request with ID %d of %d bytes from %s\n", reply.ReqId, len(reply.Data), peer.Addr)
		srv.sendToKafka(reply.Data)
	}
}

// FIXME Should support split mode and opennms mode
func (srv dialoutServer) sendToKafka(data []byte) {
	srv.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &srv.topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)
}
