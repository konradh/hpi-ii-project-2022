#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v2/corporate.proto
protoc --proto_path=proto --python_out=build/gen proto/lei/v1/leidata.proto
protoc --proto_path=proto --python_out=build/gen proto/lei/v1/leirelationshipdata.proto
