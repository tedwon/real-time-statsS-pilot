#!/bin/bash

eval "nohup storm supervisor > supervisor.log 2>&1 &"

SUPERVISOR_BACKGROUND_PID=$!
export SUPERVISOR_BACKGROUND_PID
echo $SUPERVISOR_BACKGROUND_PID > "supervisor.pid"