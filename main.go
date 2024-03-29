package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

var (
	server = dialoutServer{}
)

func main() {
	flag.StringVar(&server.bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&server.topic, "topic", "telemetry-nxos", "kafka topic that will receive the messages")
	flag.Var(&server.parameters, "param", "optional kafka producer parameter as key-value pair, for instance acks=1 (can be used multiple times)")
	flag.BoolVar(&server.onmsMode, "opennms", false, "to emulate an OpenNMS minion when sending messages to kafka")
	flag.StringVar(&server.minionID, "minion-id", "", "the ID of the minion to emulate [opennms mode only]")
	flag.StringVar(&server.minionLocation, "minion-location", "", "the location of the minion to emulate [opennms mode only]")
	flag.IntVar(&server.maxBufferSize, "max-buffer-size", 0, "maximum buffer size")
	flag.IntVar(&server.port, "port", 50001, "port to listen for gRPC requests")
	flag.BoolVar(&server.debug, "debug", false, "to display a human-readable version of the GBP paylod sent by the Nexus")
	flag.Parse()

	err := server.start()
	if err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	server.stop()
}

type properties []string

func (p *properties) String() string {
	return strings.Join(*p, ", ")
}

func (p *properties) Set(value string) error {
	*p = append(*p, value)
	return nil
}

type dialoutServer struct {
	mdt_dialout.UnimplementedGRPCMdtDialoutServer
	server         *grpc.Server
	producer       *kafka.Producer
	port           int
	debug          bool
	bootstrap      string
	topic          string
	parameters     properties
	onmsMode       bool
	maxBufferSize  int
	minionID       string
	minionLocation string
}

func (srv *dialoutServer) start() error {
	if srv.onmsMode {
		if srv.minionID == "" {
			return fmt.Errorf("minion ID is mandatory when using opennms mode")
		}
		if srv.minionLocation == "" {
			return fmt.Errorf("minion location is mandatory when using opennms mode")
		}
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.port))
	if err != nil {
		return fmt.Errorf("cannot initialize listener: %v", err)
	}

	kafkaConfig := &kafka.ConfigMap{"bootstrap.servers": srv.bootstrap}
	if srv.parameters != nil {
		for _, kv := range srv.parameters {
			array := strings.Split(kv, "=")
			if err = kafkaConfig.SetKey(array[0], array[1]); err != nil {
				return err
			}
		}
	}
	srv.producer, err = kafka.NewProducer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("could not create producer: %v", err)
	}

	go func() {
		for e := range srv.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("delivery failed: %v", ev.TopicPartition)
				} else {
					log.Printf("delivered message to %v", ev.TopicPartition)
				}
			default:
				log.Printf("kafka event: %s", ev)
			}
		}
	}()

	srv.server = grpc.NewServer()
	mdt_dialout.RegisterGRPCMdtDialoutServer(srv.server, srv)

	go func() {
		log.Printf("starting gRPC server on port %d", srv.port)
		err = srv.server.Serve(listener)
		if err != nil {
			log.Fatalf("could not serve: %v", err)
		}
	}()

	return nil
}

func (srv dialoutServer) stop() {
	log.Println("shutting down...")
	srv.server.GracefulStop()
	srv.producer.Close()
	log.Println("done!")
}

func (srv dialoutServer) MdtDialout(stream mdt_dialout.GRPCMdtDialout_MdtDialoutServer) error {
	peer, peerOK := peer.FromContext(stream.Context())
	addr := "127.0.0.1"
	if peerOK {
		log.Printf("accepted Cisco MDT GRPC dialout connection from %s", peer.Addr)
		addr = peer.Addr.String()
	}
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("dialout receive error from %s: %v", addr, err)
			return err
		}
		if len(reply.Data) == 0 && len(reply.Errors) != 0 {
			log.Printf("dialout error from %s: %s", addr, reply.Errors)
			break
		}
		log.Printf("received request with ID %d of %d bytes from %s", reply.ReqId, len(reply.Data), addr)
		srv.sendToKafka(addr, reply.Data)
	}
	if peerOK {
		log.Printf("closed Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}
	return nil
}

func (srv dialoutServer) sendToKafka(sourceAddr string, data []byte) {
	if srv.debug {
		nxosMsg := &telemetry_bis.Telemetry{}
		err := proto.Unmarshal(data, nxosMsg)
		if err == nil {
			jsonString, _ := json.MarshalIndent(nxosMsg, "", "  ")
			log.Printf("received message: %s", string(jsonString))
		} else {
			log.Println("cannot parse the payload using telemetry_bis.proto")
		}
	}
	msg := data
	if srv.onmsMode {
		msg = srv.wrapMessageToTelemetry(sourceAddr, data)
	}
	id := uuid.New().String()
	totalChunks := srv.getTotalChunks(msg)
	log.Printf("sending message of %d bytes divided into %d chunks", len(msg), totalChunks)
	var chunk int32
	for chunk = 0; chunk < totalChunks; chunk++ {
		bytes := srv.wrapMessageToSink(id, chunk, totalChunks, msg)
		log.Printf("sending chunk %d/%d to Kafka topic %s using messageId %s", chunk+1, totalChunks, srv.topic, id)
		srv.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &srv.topic, Partition: kafka.PartitionAny},
			Key:            []byte(id), // To guarantee order when having multiple partitions
			Value:          bytes,
		}, nil)
	}
}

func (srv dialoutServer) getMaxBufferSize() int32 {
	return int32(srv.maxBufferSize)
}

func (srv dialoutServer) getTotalChunks(data []byte) int32 {
	if srv.maxBufferSize == 0 {
		return int32(1)
	}
	chunks := int32(len(data) / srv.maxBufferSize)
	if len(data)%srv.maxBufferSize > 0 {
		chunks++
	}
	return chunks
}

func (srv dialoutServer) wrapMessageToSink(id string, chunk, totalChunks int32, data []byte) []byte {
	bufferSize := srv.getRemainingBufferSize(int32(len(data)), chunk)
	offset := chunk * srv.getMaxBufferSize()
	msg := data[offset : offset+bufferSize]
	sinkMsg := &sink.SinkMessage{
		MessageId:          id,
		CurrentChunkNumber: chunk,
		TotalChunks:        totalChunks,
		Content:            msg,
	}
	bytes, err := proto.Marshal(sinkMsg)
	if err != nil {
		log.Printf("error cannot serialize sink message: %v", err)
		return []byte{}
	}
	return bytes
}

func (srv dialoutServer) getRemainingBufferSize(messageSize, chunk int32) int32 {
	if srv.maxBufferSize > 0 && messageSize > srv.getMaxBufferSize() {
		remaining := messageSize - chunk*srv.getMaxBufferSize()
		if remaining > srv.getMaxBufferSize() {
			return srv.getMaxBufferSize()
		}
		return remaining
	}
	return messageSize
}

func (srv dialoutServer) wrapMessageToTelemetry(sourceAddr string, data []byte) []byte {
	log.Printf("wrapping message to emulate minion %s at location %s", srv.minionID, srv.minionLocation)
	now := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	port := uint32(srv.port)
	telemetryLogMsg := &telemetry.TelemetryMessageLog{
		SystemId:      &srv.minionID,
		Location:      &srv.minionLocation,
		SourceAddress: &sourceAddr,
		SourcePort:    &port,
		Message: []*telemetry.TelemetryMessage{
			{
				Timestamp: &now,
				Bytes:     data,
			},
		},
	}
	msg, err := proto.Marshal(telemetryLogMsg)
	if err != nil {
		log.Printf("error cannot serialize telemetry message: %v", err)
		return []byte{}
	}
	return msg
}
