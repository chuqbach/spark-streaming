#!/bin/bash

docker exec -it kafka-docker-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092
