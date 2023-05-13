# build
go build -buildmode=plugin ../mrapps/wc.go
rm mr-*
#rm mr-out*

# start worker
go run mrworker.go wc.so

# start coordinator
go run mrcoordinator.go pg-*.txt

# check output
cat mr-out-* | sort | more