#!/bin/bash

CONFIG_LOCAL=./config_local.txt
BINARY_DIR=./target/debug/
PROGRAM=project2

cargo build || exit 1

n=0
cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    while [[ $n -lt $i ]]
    do
        read line
        p=$( echo $line | awk '{ print $1 }' )
        host=$( echo $line | awk '{ print $2 }' )
        # export RUST_BACKTRACE=1
        kitty --hold -e $BINARY_DIR/$PROGRAM $CONFIG_LOCAL $p &
        n=$(( n + 1 ))
    done
    wait
)
