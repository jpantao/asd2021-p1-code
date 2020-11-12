#!/bin/sh

idx=$1
shift
java -D logFilename=logs/6/node$idx -cp asdProj.jar Main -conf config6.properties "$@" &> /proc/1/fd/1
