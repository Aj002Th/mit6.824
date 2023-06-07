current_time=`date "+%Y%m%d-%H%M%S"`
bash test-mr.sh 2>&1 | tee ../../log/lab1/${current_time}.log