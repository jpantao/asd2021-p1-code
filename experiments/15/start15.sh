#!/bin/sh

idx=$1
shift
java -D logFilename=logs/15/node$idx -cp asdProj.jar Main -conf config15.properties "$@" &> /proc/1/fd/1
