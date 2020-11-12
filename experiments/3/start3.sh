#!/bin/sh

idx=$1
shift
java -D logFilename=logs/3/node$idx -cp asdProj.jar Main -conf config3.properties "$@" &> /proc/1/fd/1
