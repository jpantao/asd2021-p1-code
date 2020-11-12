#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 || -z $2 || -z $3 || $1 != "eagerpushgossip" && $1 != "plumtree" || $2 != "cyclon" && $2 != "hyparview" ]]; then
  echo 'Usage: ./evaluate.sh <brodcast protocol> <membership protocol> <directory>'
  printf 'Broadcast protocols:\n\teagerpushgossip\n\tplumtree'
  echo
  printf 'Membership protocols:\n\thyparview\n\tcyclon'
  echo
  exit 1
fi
broadcast=$1
membership=$2
dir=$3
if [[ ! -e $dir ]]; then
  mkdir $dir
fi
nNodes=$(ls ../logs/*.log | wc -l)
function generateProtocolMetrics() {
  echo "Generating $2_Metrics.csv"
  filename="$dir/$2_Metrics.csv"
  p=1
  m="totalEvents,messagesIn,messagesFailed,messagesSent,timers,notifications,requests,replies,customChannelEvents"
  if [ $2 = "plumtree" ]; then
    echo "eagerPush,lazyPushPeers,received,missing,lazyQueue,gossipTimers,${m}" >"$filename"
    p=15
  elif [ $2 = "eagerpushgossip" ]; then
    echo "neighbours,received,${m}" >"$filename"
    p=11
  elif [ $2 = "hyparview" ]; then
    echo "activeView,passiveView,totalEvents,${m}" >"$filename"
    p=11
  elif [ $2 = "cyclon" ]; then
    echo "neighbours,upConnections,pendingConnections,pendingMsgs,sample,${m}" >"$filename"
    p=14
  fi
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  for x in $(cat ../logs/*.log | grep -o "$1" | cut -f2- -d:); do
    for k in $(echo "$x" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$k
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % p)) == 0 ]]; then
      echo $result >>"$filename"
      result=""
    fi
  done
}

function generateChannelMetrics() {
  echo "Generating $1_$2_ChannelMetrics.csv"
  filename="$dir/$1_$2_ChannelMetrics.csv"
  echo "MsgOut,BytesOut,MsgIn,BytesIn" >"$filename"
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  for x in $(cat ../logs/*.log | grep -o "ChannelMetrics.*" | cut -f2- -d:); do
    for k in $(echo "$x" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$k
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % 4)) == 0 ]]; then
      echo $result >>"$filename"
      result=""
    fi
  done
}

function calculateLatency() {
  # shellcheck disable=SC2207
  s=($(echo "$1" | tr ":" "\n" | sed 's/^0*//'))
  # shellcheck disable=SC2207
  r=($(echo "$2" | tr ":" "\n" | sed 's/^0*//'))
  # shellcheck disable=SC2004
  h=$((${r[0]} - ${s[0]}))
  # shellcheck disable=SC2004
  m=$((${r[1]} - ${s[1]}))
  # shellcheck disable=SC2004
  s=$((${r[2]} - ${s[2]}))
  # shellcheck disable=SC2004
  ms=$((${r[3]} - ${s[3]}))
  # shellcheck disable=SC2004
  echo $(($(($(($h * 60 + $m)) * 60 + $s)) * 60 + $ms))
}
function generateLatencyAndReliability() {
  echo "Generating $1_$2_reliability_latency.csv"
  echo "Reliability,Latency" >"$dir/$1_$2_reliabilityLatency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Sending" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Sending" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Received" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Received" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i++)); do
    idx=0
    l=0
    for ((k = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        idx=$k
      fi
    done
    echo "$(($l / nNodes * 100)),$(calculateLatency "${sentLatency[$i]}" "${receivedLatency[$idx]}")" >>"$dir/$1_$2_reliabilityLatency.csv"
  done
}

generateChannelMetrics $membership $broadcast
generateLatencyAndReliability $membership $broadcast
#generateProtocolMetrics 'BroadcastMetrics.*' $broadcast
#generateProtocolMetrics 'MembershipMetrics.*' $membership
