#!/bin/sh

idx=$1
shift
java -D logFilename=logs/12/node$idx -cp asdProj.jar Main -conf config12.properties "$@" &> /proc/1/fd/1
