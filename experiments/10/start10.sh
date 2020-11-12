#!/bin/sh

idx=$1
shift
java -D logFilename=logs/10/node$idx -cp asdProj.jar Main -conf config10.properties "$@" &> /proc/1/fd/1
