#!/bin/sh

idx=$1
shift
java -D logFilename=logs/23/node$idx -cp asdProj.jar Main -conf config23.properties "$@" &> /proc/1/fd/1
