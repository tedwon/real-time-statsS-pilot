#!/bin/bash

_PIDFILE="./nimbus.pid"
_PID=`cat "${_PIDFILE}"`

echo "Storm Nimbus (pid=${_PID}) is stopping..."

kill -15 $_PID

rm "${_PIDFILE}"