#!/usr/bin/env bash

GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(date --iso-8601=seconds)
GO111MODULE=on CGO_ENABLED=0 go build -ldflags "-X main.GitSHA=${GIT_SHA} -X main.BuildTime=${BUILD_TIME}"
