#!/bin/sh
echo "start news find action"
nohup /usr/local/bin/python /home/hadoop/hainiu_crawler/download_page/news_find_action_redis.py > /dev/null 2>&1 &
echo "start finish..."
