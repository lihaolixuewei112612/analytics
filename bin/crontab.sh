#!/bin/bash -l
# Don't edit this file unless you know exactly what you're doing.

BIN_DIR=$(cd $(dirname $0); pwd)
CRONTAB='/usr/bin/crontab'

$CRONTAB - << EOF
$($CRONTAB -l)

# min hour day mon weekday(0=Sunday) command
1 * * * * $BIN_DIR/dtc.sh h start
EOF

