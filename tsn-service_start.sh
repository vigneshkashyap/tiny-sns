./coordinator -p 9090 &
sleep 2s
./tsd -c 1 -s 1 -h localhost -k 9090 -p 10000 &
sleep 1s
./tsd -c 2 -s 1 -h localhost -k 9090 -p 10001 &
