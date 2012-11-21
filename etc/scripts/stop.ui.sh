#!/bin/bash

_PIDFILE="./ui.pid"
_PID=`cat "${_PIDFILE}"`

echo "Storm UI (pid=${_PID}) is stopping..."

kill -15 $_PID

rm "${_PIDFILE}"