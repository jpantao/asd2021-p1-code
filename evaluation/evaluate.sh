#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 ]]; then
  echo 'Usage: ./evaluate.sh <experiment>'
  exit 1
fi
dir="csvs/$1"
experimentPath="../logs/$1/*.log"
if [[ ! -e "csvs" ]]; then
  mkdir "csvs"
fi
if [[ ! -e "csvs/$1" ]]; then
  mkdir "csvs/$1"
fi
if [[ ! -e $dir ]]; then
  mkdir $dir
fi
nNodes=$(ls $experimentPath | wc -l)
echo "nodes: $nNodes"

# shellcheck disable=SC2126
echo "Broadcasts sent: $(grep "BroadcastApp" $experimentPath | grep "Sending" | wc -l)"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "node$i received: $(grep "BroadcastApp" "../logs/$1/node$i.log" | grep "Received" | wc -l)"
done

function generateChannelMetrics() {
  echo "Generating ChannelMetrics.csv"
  filename="$dir/ChannelMetrics.csv"
  echo "InConnections,,,, InConnections (Old),,,, OutConnections,,,, OutConnections(Old)" >$filename
  echo "MsgOut,BytesOut,MsgIn,BytesIn,MsgOut,BytesOut,MsgIn,BytesIn,MsgOut,BytesOut,MsgIn,BytesIn,MsgOut,BytesOut,MsgIn,BytesIn," >>$filename
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  for x in $(cat $experimentPath | grep -o "ChannelMetrics.*" | cut -f2- -d:); do
    for k in $(echo "$x" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$k
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % 16)) == 0 ]]; then
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
function generateLatencyAndReliability() {
  echo "Generating reliability_latency.csv"
  echo "Reliability, SentTime, ReceivedTime" >"$dir/reliabilityLatency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i++)); do
    l=0
    latencies="$(convertToMillis ${receivedLatency[$i]}),"
    for ((k = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        latencies="${latencies},$(convertToMillis ${receivedLatency[$k]})"
      fi
    done

    echo "$(($l * 100 / nNodes)), $latencies" >>"$dir/reliabilityLatency.csv"
  done
}

#generateChannelMetrics
generateLatencyAndReliability
