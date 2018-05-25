#!/usr/bin/env bash

GIT_SHA=$(git rev-parse HEAD || echo "GitNotFound")
BUILD_TIME=$(date --rfc-3339=seconds | tr ' ' 'T')
go build -ldflags "-X main.GitSHA=${GIT_SHA} -X main.BuildTime=${BUILD_TIME}"
