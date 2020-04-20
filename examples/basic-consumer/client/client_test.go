// @author Alejandro Galue <agalue@opennms.org>

package client

import (
	"fmt"
	"sync"
	"testing"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/golang/protobuf/proto"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gotest.tools/assert"
)

func TestProcessMessage(t *testing.T) {
	cli := &KafkaClient{}
	cli.createVariables()
	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			executeTest(t, cli, fmt.Sprintf("ID%d", index))
		}(i)
	}
	wg.Wait()
}

func executeTest(t *testing.T, cli *KafkaClient, id string) {
	var data []byte
	data = cli.processMessage(buildMessage(id, 0, 3, []byte("ABC")))
	assert.Assert(t, data == nil)
	data = cli.processMessage(buildMessage(id, 1, 3, []byte("DEF")))
	assert.Assert(t, data == nil)
	data = cli.processMessage(buildMessage(id, 1, 3, []byte("DEF")))
	assert.Assert(t, data == nil)
	data = cli.processMessage(buildMessage(id, 2, 3, []byte("GHI")))
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
