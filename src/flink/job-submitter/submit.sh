#!/bin/sh
set -e

JOBMANAGER_URL="${JOBMANAGER_URL:-http://flink-jobmanager:8081}"
JAR_PATH="/opt/job/flink-job-submitter.jar"
ENTRY_CLASS="FlinkJobSubmitter"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Waiting for JobManager at ${JOBMANAGER_URL}..."
for i in $(seq 1 $MAX_RETRIES); do
    if curl -sf "${JOBMANAGER_URL}/overview" > /dev/null 2>&1; then
        echo "JobManager is ready (attempt ${i})."
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "ERROR: JobManager not ready after ${MAX_RETRIES} attempts."
        exit 1
    fi
    echo "  attempt ${i}/${MAX_RETRIES} - not ready yet"
    sleep $RETRY_INTERVAL
done

echo ""
echo "Uploading JAR to JobManager..."
UPLOAD_RESPONSE=$(curl -s -X POST "${JOBMANAGER_URL}/jars/upload" \
    -H "Expect:" \
    -F "jarfile=@${JAR_PATH}")

echo "Upload response: ${UPLOAD_RESPONSE}"

JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*"' | sed 's/"filename":"//;s/"//g' | awk -F'/' '{print $NF}')

if [ -z "$JAR_ID" ]; then
    echo "ERROR: Failed to extract JAR ID from upload response."
    exit 1
fi

echo "JAR uploaded: ${JAR_ID}"
echo ""
echo "Running job..."
RUN_RESPONSE=$(curl -s -X POST "${JOBMANAGER_URL}/jars/${JAR_ID}/run" \
    -H "Content-Type: application/json" \
    -d "{\"entryClass\": \"${ENTRY_CLASS}\"}")

echo "Run response: ${RUN_RESPONSE}"

JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//g')

if [ -z "$JOB_ID" ]; then
    echo "ERROR: Failed to start job."
    exit 1
fi

echo ""
echo "Job submitted successfully! Job ID: ${JOB_ID}"
echo "Monitor at: ${JOBMANAGER_URL}/#/job/${JOB_ID}"