package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// WARNING This is for test purposes only
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
	defer stream.CloseSend()

	err = stream.Send(&mdt_dialout.MdtDialoutArgs{
		ReqId: 1,
		Data:  getTelemetryBytes(),
	})
	if err != nil {
		log.Fatalf("cannot send telemetry data: %v", err)
	}

	time.Sleep(5 * time.Second) // To avoid session errors on the server
	log.Println("done!")
}

func getTelemetryBytes() []byte {
	telemetry := &telemetry_bis.Telemetry{
		MsgTimestamp: 1543236572000,
		EncodingPath: "type:test",
		NodeId:       &telemetry_bis.Telemetry_NodeIdStr{NodeIdStr: "hostname"},
		Subscription: &telemetry_bis.Telemetry_SubscriptionIdStr{SubscriptionIdStr: "subscription"},
		DataGpbkv: []*telemetry_bis.TelemetryField{
			{
				Fields: []*telemetry_bis.TelemetryField{
					{
						Name: "content",
						Fields: []*telemetry_bis.TelemetryField{
							{
								Name:        "owner",
								ValueByType: &telemetry_bis.TelemetryField_StringValue{StringValue: "agalue"},
							},
						},
					},
				},
			},
		},
	}
	fmt.Printf("sending %s", proto.MarshalTextString(telemetry))
	data, _ := proto.Marshal(telemetry)
	return data
}
