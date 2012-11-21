#!/bin/bash

eval "nohup storm nimbus > nimbus.log 2>&1 &"


NIMBUS_BACKGROUND_PID=$!
export NIMBUS_BACKGROUND_PID
echo $NIMBUS_BACKGROUND_PID > "nimbus.pid"