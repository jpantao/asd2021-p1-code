#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 ]]; then
  echo 'Usage: ./evaluate.sh <experiment>'
  exit 1
fi
experiment=$1
run=$2
experimentPath="../logs/run$run/$experiment/*.log"
channelMetrics="csvs/channelMetrics"
latencies="csvs/latencies"
reliabilities="csvs/reliabilities"

mkdir -p $channelMetrics
mkdir -p $latencies
mkdir -p $reliabilities

nNodes=$(ls $experimentPath | wc -l)
echo "Generating reliability.csv"
# shellcheck disable=SC2126
echo "Total:,$(grep "BroadcastApp" $experimentPath | grep "Sending" | wc -l)" >>"$reliabilities/'$experiment'reliability.csv"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "$i,$(grep "BroadcastApp" "../logs/run$run/$experiment/node$i.log" | grep "Received" | wc -l)" >>"$reliabilities/'$experiment'reliability.csv"
done

function generateChannelMetrics() {
  echo "Generating ChannelMetrics.csv"
  filename="$channelMetrics/'$experiment'ChannelMetrics.csv"
  declare -a metrics
  i=0
  l=0
  IFS='
  '
  # shellcheck disable=SC2013
  # shellcheck disable=SC2207
  aux=($(cat $experimentPath | grep -o "ChannelMetrics.*" | cut -f2- -d:))
  for ((k = 0; k < ${#aux[@]}; k += 1)); do
    for j in $(echo "${aux[$k]}" | tr "=" "\n" | grep "[0-9]" | tr -d "[a-zA-Z]"); do
      l=$((l + 1))
      metrics[l]=$j
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % 8)) == 0 ]]; then
      echo $result >>"$filename"
      result=""
    fi
  done
}

function convertToMillis() {
  # shellcheck disable=SC2207
  a=($(echo "$1" | tr ":" "\n" | awk '{x=$0+0;print x}'))
  echo $(($(($((${a[0]} * 60 + ${a[1]})) * 60 + ${a[2]})) * 60 + ${a[3]}))
}
function generateLatency() {
  echo "Generating latency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i += nNodes/2)); do
    l=0
    latencies_="$(convertToMillis ${sentLatency[$i]})"
    for ((k = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        latencies_="${latencies_},$(convertToMillis ${receivedLatency[$k]})"
      fi
    done
    echo $latencies_ >>"$latencies/'$experiment'latency.csv"
  done
}

#generateChannelMetrics
generateLatency
