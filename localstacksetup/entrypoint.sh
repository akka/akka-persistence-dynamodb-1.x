#!/bin/sh
set -o errexit -o nounset -o pipefail

sleep 5

while ! nc -z "$LOCALSTACK_HOST" "$DDB_PORT"; do sleep 3; done

export DYNAMODB_ENDPOINT="http://${LOCALSTACK_HOST}:${DDB_PORT}"

echo "=== \$AWS_REGION = $AWS_REGION ==="
echo "=== \$LOCALSTACK_HOST = $LOCALSTACK_HOST ==="
echo "=== \$DDB_PORT = $DDB_PORT ==="
echo "=== \$DYNAMODB_ENDPOINT = $DYNAMODB_ENDPOINT ==="
echo "=== \$DDB_TABLE_COUNTERS = $DDB_TABLE_JOURNAL ==="
echo "=== \$DDB_TABLE_TRANSACTIONS = $DDB_TABLE_SNAPSHOT ==="

DIR=$(dirname "$0")

"$DIR"/create-tables.sh
