#!/bin/sh
experiment=$1
idx=$2
user=$3
run=$4
shift 3

logsfilename="logs/$run/$experiment/node$idx"
java -DlogFilename="$logsfilename" -cp asdProj.jar Main -conf "config$experiment.properties" "$@" &>/proc/1/fd/1
chown $user logs/$experiment/node$idx.log
