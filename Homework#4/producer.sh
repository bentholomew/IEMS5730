#!/bin/bash
while read line
do
	sleep 1
    echo $line | bin/kafka-console-producer.sh --broker-list kafka-headless:9092 --topic tweets
done < new_tweets.txt