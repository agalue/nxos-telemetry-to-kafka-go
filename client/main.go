package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"google.golang.org/grpc"
)

func main() {
	backend := flag.String("b", "localhost:50001", "addres of the nx-os grpc backend")

	conn, err := grpc.Dial(*backend, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect to %s: %v", *backend, err)
	}
	defer conn.Close()

	client := mdt_dialout.NewGRPCMdtDialoutClient(conn)
	stream, err := client.MdtDialout(context.Background())
	if err != nil {
		log.Fatalf("could not connect to dialout: %v", err)
	}

	req := &mdt_dialout.MdtDialoutArgs{
		ReqId: 1,
		Data:  []byte("Go is awesome!"),
	}
	go stream.Send(req)
	go stream.Recv()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Println("shutting down...")
	stream.CloseSend()
	log.Println("done!")
}
