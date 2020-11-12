#!/bin/sh

idx=$1
shift
java -D logFilename=logs/18/node$idx -cp asdProj.jar Main -conf config18.properties "$@" &> /proc/1/fd/1
