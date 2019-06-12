#!/bin/bash
while [ 1 ];
do
	count=`ls -l core.* |wc -l`
	if [ $count -lt 3 ]
	then
		ulimit -c unlimited
	else
		ulimit -c 0
	fi

	./object_scanner  >> /dev/null
	echo program crashed, restart at `date +"%w %Y/%m/%d, %H:%M:%S"` >> RESTART.log
	sleep 30
done

