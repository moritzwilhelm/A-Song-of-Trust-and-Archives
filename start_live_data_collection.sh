#!/bin/bash

source $(dirname "$0")/linux-venv/bin/activate
cd $(dirname "$0")/src
echo "START CRAWL"
python3 data_collection/collect_live_data.py
echo "FINISHED CRAWL"
cd $(dirname "$0")
deactivate
