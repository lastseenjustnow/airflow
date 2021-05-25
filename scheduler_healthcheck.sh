#!/bin/bash
while sleep 60; do
    echo "Checking scheduler status..."
    v=$(curl -s 10.2.0.21:8080/health | jq '.scheduler.status')
    v="${v%\"}"
    SCHEDULER_STATUS="${v#\"}"
    echo "$SCHEDULER_STATUS"
    if [ "$SCHEDULER_STATUS" = "unhealthy" ]; then
        echo "AIRFLOW SCHEDULER RESTARTING"
        docker exec airflow airflow scheduler
    fi
    done