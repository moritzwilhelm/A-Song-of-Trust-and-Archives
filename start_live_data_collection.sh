#!/bin/bash

source $(dirname "$0")/linux-venv/bin/activate
cd $(dirname "$0")/src
echo "<<< START CRAWLING >>>"
python3 data_collection/collect_live_data.py
echo "<<< FINISHED CRAWLING >>>"
cd $(dirname "$0")
deactivate
