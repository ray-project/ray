#!/usr/bin/env bash

function  for_in_file(){
    if [ -f $1 ]; then
        echo "THIS IS THE NEXT TASKID HERE:\n" >> logfilter
        b="nextTaskId:"
        for  i  in  `cat $1`
        do
            grep -r $i *|awk -v prefix=$b '/Task .+ Object .+ get/{print prefix$(6) >> "logfilter"}'
        done
    fi
}

cd ./$1/
if [ -f logfilter ]; then
    rm logfilter
fi
if [ -f tempobjectid ]; then
    rm tempobjectid
fi
grep -r "$2" * > logfilter
cat logfilter|awk '/Task .+ Object .+ put/{print $(NF-1) > "tempobjectid"}'
for_in_file tempobjectid
cat logfilter



