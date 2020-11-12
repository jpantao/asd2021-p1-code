#!/bin/sh

idx=$1
shift
java -D logFilename=logs/5/node$idx -cp asdProj.jar Main -conf config5.properties "$@" &> /proc/1/fd/1
