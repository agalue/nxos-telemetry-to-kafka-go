package main

import (
	"testing"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gotest.tools/assert"
)

func TestProcessMessage(t *testing.T) {
	cli := KafkaClient{}
	cli.msgBuffer = make(map[string][]byte)
	cli.chunkTracker = make(map[string]int32)

	var data []byte
	id := uuid.New().String()
	data = cli.ProcessMessage(buildMessage(id, 0, 3, []byte("ABC")))
	assert.Assert(t, data == nil)
	data = cli.ProcessMessage(buildMessage(id, 1, 3, []byte("DEF")))
	assert.Assert(t, data == nil)
	data = cli.ProcessMessage(buildMessage(id, 1, 3, []byte("DEF")))
	assert.Assert(t, data == nil)
	data = cli.ProcessMessage(buildMessage(id, 2, 3, []byte("GHI")))
	assert.Assert(t, data != nil)
	assert.Equal(t, "ABCDEFGHI", string(data))
}

func buildMessage(id string, chunk, total int32, data []byte) *kafka.Message {
	topic := "Test"
	sinkMsg := &sink.SinkMessage{
		MessageId:          &id,
		CurrentChunkNumber: &chunk,
		TotalChunks:        &total,
		Content:            data,
	}
	bytes, _ := proto.Marshal(sinkMsg)
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Key:            []byte(id),
		Value:          bytes,
	}
	return kafkaMsg
}
