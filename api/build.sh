#!/bin/bash

type protoc >/dev/null 2>&1 || { echo >&2 "protoc required but it's not installed; aborting."; exit 1; }

protoc -I . sink.proto --go_out=./sink
protoc -I . telemetry.proto --go_out=./telemetry
protoc -I . telemetry_bis.proto --go_out=./telemetry_bis
protoc -I . mdt_dialout.proto --go_out=plugins=grpc:./mdt_dialout
