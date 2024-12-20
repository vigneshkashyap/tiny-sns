#!/bin/bash

# need to run this command below
# chmod +x startup.sh

# Start the coordinator
GLOG_logtostderr=1 ./coordinator -p 9000 &

sleep 2

echo "STARTING TSD PROCESSES"
# Start the tsd processes

./tsd -c 1 -s 1 -h localhost -k 9000 -p 10000 &
./tsd -c 2 -s 1 -h localhost -k 9000 -p 10001 &
./tsd -c 3 -s 1 -h localhost -k 9000 -p 20000 &

sleep 1

./tsd -c 1 -s 2 -h localhost -k 9000 -p 20001 &
./tsd -c 2 -s 2 -h localhost -k 9000 -p 30000 &
./tsd -c 3 -s 2 -h localhost -k 9000 -p 30001 &


sleep 1

echo "STARTING SYNCHRONIZER PROCESSES"

GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9001 -i 1 &
GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9002 -i 2 &
GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9003 -i 3 &

sleep 1

GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9004 -i 4 &
GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9005 -i 5 &
GLOG_logtostderr=1 ./synchronizer -h localhost -k 9000 -p 9006 -i 6 &
