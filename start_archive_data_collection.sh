#!/bin/bash

source $(dirname "$0")/linux-venv/bin/activate
cd $(dirname "$0")/src
echo "<<< START CRAWLING >>>"
python3 data_collection/proxy_crawl_manager.py daily_archive -p $@
echo "<<< FINISHED CRAWLING >>>"
cd $(dirname "$0")
deactivate
