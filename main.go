package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/mdt_dialout"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry_bis"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	version = "v0.1.0"
	server  = dialoutServer{
		minionAddress: "127.0.0.1", // FIXME
	}
)

func main() {
	flag.StringVar(&server.bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap server")
	flag.StringVar(&server.topic, "topic", "telemetry-nxos", "kafka topic that will receive the messages")
	flag.Var(&server.parameters, "param", "kafka producer parameters (e.x. acks=1), can be specified multiple times")
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

type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type dialoutServer struct {
	server         *grpc.Server
	producer       *kafka.Producer
	port           int
	debug          bool
	bootstrap      string
	topic          string
	parameters     arrayFlags
	onmsMode       bool
	maxBufferSize  int
	minionID       string
	minionLocation string
	minionAddress  string
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
		return fmt.Errorf("could not listen to port %d: %v", srv.port, err)
	}

	kafkaConfig := &kafka.ConfigMap{"bootstrap.servers": srv.bootstrap}
	for _, kv := range srv.parameters {
		array := strings.Split(kv, "=")
		if err = kafkaConfig.SetKey(array[0], array[1]); err != nil {
			return err
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
					log.Printf("delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	srv.server = grpc.NewServer()
	mdt_dialout.RegisterGRPCMdtDialoutServer(srv.server, srv)

	go func() {
		log.Printf("starting gRPC server on port %d\n", srv.port)
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

func (srv dialoutServer) sendToKafka(data []byte) {
	if srv.debug {
		nxosMsg := &telemetry_bis.Telemetry{}
		err := proto.Unmarshal(data, nxosMsg)
		if err == nil {
			log.Printf("received message:\n%s", proto.MarshalTextString(nxosMsg))
		} else {
			log.Println("cannot parse the payload using telemetry_bis.proto")
		}
	}
	msg := data
	if srv.onmsMode {
		msg = srv.wrapMessageToTelemetry(data)
	}
	id := uuid.New().String()
	totalChunks := srv.getTotalChunks(msg)
	log.Printf("sending message of %d bytes divided into %d chunks\n", len(msg), totalChunks)
	var chunk int32
	for chunk = 0; chunk < totalChunks; chunk++ {
		chunkID := chunk + 1
		bytes := srv.wrapMessageToSink(id, chunk, totalChunks, msg)
		log.Printf("sending chunk %d/%d to Kafka topic %s using messageId %s", chunkID, totalChunks, srv.topic, id)
		srv.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &srv.topic, Partition: kafka.PartitionAny},
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
	chunks := int32(math.Ceil(float64(len(data) / srv.maxBufferSize)))
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
		MessageId:          &id,
		CurrentChunkNumber: &chunk,
		TotalChunks:        &totalChunks,
		Content:            msg,
	}
	bytes, err := proto.Marshal(sinkMsg)
	if err != nil {
		log.Printf("error cannot serialize sink message: %v\n", err)
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

func (srv dialoutServer) wrapMessageToTelemetry(data []byte) []byte {
	log.Printf("wrapping message to emulate minion %s at location %s\n", srv.minionID, srv.minionLocation)
	now := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	port := uint32(srv.port)
	telemetryLogMsg := &telemetry.TelemetryMessageLog{
		SystemId:      &srv.minionID,
		Location:      &srv.minionLocation,
		SourceAddress: &srv.minionAddress,
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
		log.Printf("error cannot serialize telemetry message: %v\n", err)
		return []byte{}
	}
	return msg
}
