#!/bin/sh

idx=$1
shift
java -D logFilename=logs/1/node$idx -cp asdProj.jar Main -conf config1.properties "$@" &> /proc/1/fd/1
