#!/bin/bash
#Start server zookeeper
nohup /home/ec2-user/kafka_2.10-0.8.2.1/bin/zookeeper-server-start.sh /home/ec2-user/kafka_2.10-0.8.2.1/config/zookeeper.properties &

sleep 1m

#Start server kafka
nohup /home/ec2-user/kafka_2.10-0.8.2.1/bin/kafka-server-start.sh /home/ec2-user/kafka_2.10-0.8.2.1/config/server.properties &

#cd /home/ec2-user/kafka-web-console/
#nohoup /home/ec2-user/kafka-web-console/activator start &
