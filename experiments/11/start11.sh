#!/bin/sh

idx=$1
shift
java -D logFilename=logs/11/node$idx -cp asdProj.jar Main -conf config11.properties "$@" &> /proc/1/fd/1
