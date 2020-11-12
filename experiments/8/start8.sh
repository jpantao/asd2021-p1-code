#!/bin/sh

idx=$1
shift
java -D logFilename=logs/8/node$idx -cp asdProj.jar Main -conf config8.properties "$@" &> /proc/1/fd/1
