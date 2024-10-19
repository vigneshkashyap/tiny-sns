GLOG_logtostderr=1 ./coordinator -p 9090
sleep 2s
GLOG_logtostderr=1 ./tsd -c 1 -s 1 -h localhost -k 9090 -p 10000
sleep 1s
GLOG_logtostderr=1 ./tsd -c 2 -s 1 -h localhost -k 9090 -p 10001
