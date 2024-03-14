package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// WARNING This is for test purposes only
func main() {
	backend := flag.String("b", "localhost:50001", "addres of the nx-os grpc backend")
	fields := flag.Int("n", 5, "number of sample fields")
	total := flag.Int("t", 10, "number of total messages to send")
	wait := flag.Duration("w", 1*time.Second, "number of seconds to wait between messages")
	flag.Parse()

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
	defer stream.CloseSend()

	for i := 0; i < *total; i++ {
		err = stream.Send(&mdt_dialout.MdtDialoutArgs{
			ReqId: 1,
			Data:  getTelemetryBytes(*fields),
		})
		if err != nil {
			log.Printf("[error] cannot send telemetry data: %v", err)
		}
		time.Sleep(*wait)
	}

	time.Sleep(5 * time.Second) // To avoid session errors on the server
	log.Println("done!")
}

func getTelemetryBytes(numFields int) []byte {
	fields := []*telemetry_bis.TelemetryField{
		{
			Name:        "owner",
			ValueByType: &telemetry_bis.TelemetryField_StringValue{StringValue: "agalue"},
		},
	}
	for i := 0; i < numFields; i++ {
		fields = append(fields, &telemetry_bis.TelemetryField{
			Name:        fmt.Sprintf("k%d", i),
			ValueByType: &telemetry_bis.TelemetryField_StringValue{StringValue: fmt.Sprintf("v%d", i)},
		})
	}
	telemetry := &telemetry_bis.Telemetry{
		MsgTimestamp: makeTimestamp(),
		EncodingPath: "type:test",
		NodeId:       &telemetry_bis.Telemetry_NodeIdStr{NodeIdStr: "hostname"},
		Subscription: &telemetry_bis.Telemetry_SubscriptionIdStr{SubscriptionIdStr: "subscription"},
		DataGpbkv: []*telemetry_bis.TelemetryField{
			{
				Fields: fields,
			},
		},
	}
	data, _ := proto.Marshal(telemetry)
	return data
}

func makeTimestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}
