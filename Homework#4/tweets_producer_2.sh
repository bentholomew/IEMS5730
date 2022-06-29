#!/bin/bash
while read line
do
	sleep $(($RANDOM%4+1))
    echo $line | /usr/hdp/2.6.5.0-292/kafka/bin/kafka-console-producer.sh --broker-list dicvmd7.ie.cuhk.edu.hk:6667 --topic 1155162635_hw4_2
done < new_tweets.txt