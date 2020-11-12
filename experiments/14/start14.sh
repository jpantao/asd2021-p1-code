#!/bin/sh

idx=$1
shift
java -D logFilename=logs/14/node$idx -cp asdProj.jar Main -conf config14.properties "$@" &> /proc/1/fd/1
