#!/bin/sh

idx=$1
shift
java -D logFilename=logs/16/node$idx -cp asdProj.jar Main -conf config16.properties "$@" &> /proc/1/fd/1
