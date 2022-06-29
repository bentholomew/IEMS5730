#!/usr/bin/python3
import os
import random
import time

def random_sleep():
    t = random.randint(1, 4)
    time.sleep(t)

def main():
    with open('new_tweets.txt','r',encoding='utf-8') as f:
        for line in f:
            # print(line)
            cmd = 'echo "' + line + '" | bin/kafka-console-producer.sh --broker-list kafka-headless:9092 --topic tweets'
            # print(cmd)
            os.system(cmd)
            random_sleep()


if __name__ == '__main__':
    main()