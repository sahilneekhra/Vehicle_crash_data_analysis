#!/bin/bash

###################################################################
#
# This script can be used if project needs to be deployed on PROD
#
###################################################################

spark-submit \
--master loca['*'] \
src/main/main.py