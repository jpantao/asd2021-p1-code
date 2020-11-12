#!/bin/sh

idx=$1
shift
java -D logFilename=logs/2/node$idx -cp asdProj.jar Main -conf config2.properties "$@" &> /proc/1/fd/1
