#!/bin/bash

eval "nohup storm ui > ui.log 2>&1 &"


UI_BACKGROUND_PID=$!
export UI_BACKGROUND_PID
echo $UI_BACKGROUND_PID > "ui.pid"