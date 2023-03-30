#!/bin/bash

# Change this to your netid
netid=ash170000
#netid=rdc180001

# Root directory of your project
PROJECT_DIR=/home/013/a/as/ash170000/CS-6380/project2
#PROJECT_DIR=/home/013/r/rd/rdc180001/DCProject1

# Directory where the config file is located on your local system
CONFIG_LOCAL=./config.txt
CONFIG_REMOTE=$PROJECT_DIR/config.txt

# Directory your java classes are in
BINARY_DIR=$PROJECT_DIR/

# Your main project class
PROGRAM=$BINARY_DIR/project2

n=0

if [ -e ./src/main.rs ]; then
    cargo build --target x86_64-unknown-linux-musl -r || exit 1
    scp ./target/x86_64-unknown-linux-musl/release/project2 $netid@dc01:$BINARY_DIR
fi
exit

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
  read i
  echo $i
  while [[ $n -lt $i ]]
  do
    read line
    p=$( echo $line | awk '{ print $1 }' )
    host=$( echo $line | awk '{ print $2 }' )
    kitty --hold -e ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host "$PROGRAM $CONFIG_REMOTE $p" &
    n=$(( n + 1 ))
  done
  wait
)
