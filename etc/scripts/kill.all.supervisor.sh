#!/bin/bash
 
jps | grep supervisor | awk '{print $1}' | xargs kill -15
