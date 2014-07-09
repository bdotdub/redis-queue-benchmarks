#!/bin/bash

go run benchmark.go | tee results-`date +%Y%m%d%H%M`.txt
