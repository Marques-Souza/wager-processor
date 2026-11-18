import { createHash } from 'node:crypto';
import { MikroORM } from '@mikro-orm/core';
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import mikroOrmConfig from '../../src/infrastructure/persistence/mikro-orm.config';
import {
  SubmitWagerCommand,
  WageringService,
} from '../../src/application/wagering/wagering.service';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs-client';
import { WagerTransactionKind } from '../../src/domain/entities/wager-transaction';

function parseTransaction(value: Record<string, unknown>): SubmitWagerCommand {
  const required = (field: string): string => {
    const item = value[field];
    if (typeof item !== 'string') {
      throw new Error(`Transaction is missing ${field}`);
    }
    return item;
  };
  const money = value.money;
  if (money === null || typeof money !== 'object' || Array.isArray(money)) {
    throw new Error('Transaction is missing money');
  }
  const moneyRecord = money as Record<string, unknown>;
  const amount = moneyRecord.amount;
  const currency = moneyRecord.currency;
  const kind = value.kind;
  if (
    typeof amount !== 'string' ||
    typeof currency !== 'string' ||
    typeof kind !== 'string' ||
    !Object.values(WagerTransactionKind).includes(
      kind as WagerTransactionKind,
    ) ||
    kind === WagerTransactionKind.Opening
  ) {
    throw new Error('Transaction has invalid kind or money');
  }
  const reference = value.referenceExternalTransactionId;
  if (reference !== undefined && typeof reference !== 'string') {
    throw new Error('Transaction has invalid reference');
  }
  return {
    providerId: required('providerId'),
    externalTransactionId: required('externalTransactionId'),
    playerId: required('playerId'),
    walletId: required('walletId'),
    roundId: required('roundId'),
    gameId: required('gameId'),
    kind: kind as SubmitWagerCommand['kind'],
    money: { amount, currency },
    ...(reference === undefined
      ? {}
      : { referenceExternalTransactionId: reference }),
  };
}

async function main(): Promise<void> {
  const queueUrl = process.argv[2];
  if (queueUrl === undefined) {
    throw new Error('Queue URL is required');
  }

  const sqsClient = createSqsClient();
  const orm = await MikroORM.init(mikroOrmConfig);
  try {
    const response = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
        VisibilityTimeout: 30,
      }),
    );
    const message = response.Messages?.[0];
    if (
      message?.MessageId === undefined ||
      message.ReceiptHandle === undefined ||
      message.Body === undefined
    ) {
      throw new Error('Worker did not receive an SQS message');
    }

    const envelope = JSON.parse(message.Body) as {
      data: Record<string, unknown>;
    };
    const idempotencyKey = envelope.data.idempotencyKey;
    if (typeof idempotencyKey !== 'string') {
      throw new Error('Received message does not contain a wager');
    }
    const result = await new WageringService(orm.em).submitWager(
      parseTransaction(envelope.data),
      idempotencyKey,
      {
        consumerName: 'wager-transactions',
        messageId: message.MessageId,
        payloadHash: createHash('sha256').update(message.Body).digest('hex'),
      },
    );

    process.stdout.write(
      `${JSON.stringify({
        result,
        receiptHandle: message.ReceiptHandle,
      })}\n`,
    );
  } finally {
    await orm.close(true);
    sqsClient.destroy();
  }
}

void main();
