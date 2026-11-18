#!/bin/bash
set -eu

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

dlq_arn="$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)"

attributes="$(python3 -c 'import json,sys; print(json.dumps({"FifoQueue":"true","ContentBasedDeduplication":"false","RedrivePolicy":json.dumps({"deadLetterTargetArn":sys.argv[1],"maxReceiveCount":"5"})}))' "$dlq_arn")"
awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$attributes"

awslocal sqs create-queue \
  --queue-name wager-processor-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'
