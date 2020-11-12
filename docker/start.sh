#!/bin/sh
experiment=$1
idx=$2
logsfilename="logs/$experiment/node$idx"
shift 2
java -D logFilename=logs/node$idx -cp asdProj.jar Main -conf "config$experiment.properties" "$@" &>/proc/1/fd/1
