#!/bin/bash

_PIDFILE="./supervisor.pid"
_PID=`cat "${_PIDFILE}"`

echo "Storm Supervisor (pid=${_PID}) is stopping..."

kill -15 $_PID

rm "${_PIDFILE}"