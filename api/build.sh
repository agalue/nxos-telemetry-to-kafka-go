#!/bin/bash

type protoc >/dev/null 2>&1 || { echo >&2 "protoc required but it's not installed; aborting."; exit 1; }

protoc --proto_path=./ --go_out=./ sink.proto
protoc --proto_path=./ --go_out=./ telemetry.proto
protoc --proto_path=./ --go_out=./ telemetry_bis.proto
protoc --proto_path=./ --go_out=plugins=grpc:./ mdt_dialout.proto
