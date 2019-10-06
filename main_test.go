package main

import (
	"testing"
	"time"

	"github.com/agalue/nxos-telemetry-to-kafka-go/api/sink"
	"github.com/agalue/nxos-telemetry-to-kafka-go/api/telemetry"
	"github.com/golang/protobuf/proto"
	"gotest.tools/assert"
)

func TestTotalChunks(t *testing.T) {
	srv := dialoutServer{}
	assert.Equal(t, int32(0), srv.maxBufferSize)
	assert.Equal(t, int32(1), srv.getTotalChunks([]byte("test")))
	srv.maxBufferSize = 4
	assert.Equal(t, int32(2), srv.getTotalChunks([]byte("abcdEFG")))
	assert.Equal(t, int32(2), srv.getTotalChunks([]byte("abcdEFGH")))
	assert.Equal(t, int32(3), srv.getTotalChunks([]byte("abcdEFGHij")))
}

func TestGetRemainingBufferSize(t *testing.T) {
	srv := dialoutServer{
		maxBufferSize: 4,
	}
	data := []byte("abcdEFGHij")
	size := int32(len(data))
	assert.Equal(t, int32(4), srv.getRemainingBufferSize(size, 0))
	assert.Equal(t, int32(4), srv.getRemainingBufferSize(size, 1))
	assert.Equal(t, int32(2), srv.getRemainingBufferSize(size, 2))
}

func TestWrapMessageToProto(t *testing.T) {
	srv := dialoutServer{
		maxBufferSize: 4,
	}
	data := []byte("abcdEFGHij")
	totalChunks := srv.getTotalChunks(data)

	msg := srv.wrapMessageToProto("T1", 0, totalChunks, data)
	sinkMsg := &sink.SinkMessage{}
	proto.Unmarshal(msg, sinkMsg)
	assert.Equal(t, "T1", sinkMsg.GetMessageId())
	assert.Equal(t, totalChunks, sinkMsg.GetTotalChunks())
	assert.Equal(t, int32(0), sinkMsg.GetCurrentChunkNumber())
	assert.Equal(t, "abcd", string(sinkMsg.GetContent()))

	msg = srv.wrapMessageToProto("T2", 1, totalChunks, data)
	sinkMsg = &sink.SinkMessage{}
	proto.Unmarshal(msg, sinkMsg)
	assert.Equal(t, "T2", sinkMsg.GetMessageId())
	assert.Equal(t, totalChunks, sinkMsg.GetTotalChunks())
	assert.Equal(t, int32(1), sinkMsg.GetCurrentChunkNumber())
	assert.Equal(t, "EFGH", string(sinkMsg.GetContent()))

	msg = srv.wrapMessageToProto("T3", 2, totalChunks, data)
	sinkMsg = &sink.SinkMessage{}
	err := proto.Unmarshal(msg, sinkMsg)
	assert.NilError(t, err)
	assert.Equal(t, "T3", sinkMsg.GetMessageId())
	assert.Equal(t, totalChunks, sinkMsg.GetTotalChunks())
	assert.Equal(t, int32(2), sinkMsg.GetCurrentChunkNumber())
	assert.Equal(t, "ij", string(sinkMsg.GetContent()))
}

func TestBuildTelemetryMessage(t *testing.T) {
	data := []byte("opennms")
	srv := dialoutServer{
		maxBufferSize:  4,
		onmsMode:       true,
		port:           50001,
		minionID:       "minion01",
		minionLocation: "Apex",
	}
	msg := srv.buildTelemetryMessage(data)
	logMsg := &telemetry.TelemetryMessageLog{}
	err := proto.Unmarshal(msg, logMsg)
	assert.NilError(t, err)
	assert.Equal(t, "Apex", logMsg.GetLocation())
	assert.Equal(t, "minion01", logMsg.GetSystemId())
	assert.Equal(t, uint32(50001), logMsg.GetSourcePort())
	assert.Equal(t, 1, len(logMsg.GetMessage()))
	telMsg := logMsg.GetMessage()[0]
	assert.Equal(t, "opennms", string(telMsg.GetBytes()))
	// Timestamp is expected in milliseconds, should be converted to nanoseconds
	telTime := time.Unix(0, int64(telMsg.GetTimestamp())*1000000)
	assert.Equal(t, time.Now().Format("2006-01-02"), telTime.Format("2006-01-02"))
}
