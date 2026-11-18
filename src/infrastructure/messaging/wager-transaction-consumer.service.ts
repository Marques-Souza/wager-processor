import { createHash } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  HttpException,
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { MetricsService } from '../../application/observability/metrics.service';
import {
  SubmitWagerCommand,
  WageringService,
} from '../../application/wagering/wagering.service';
import { WagerTransactionKind } from '../../domain/entities/wager-transaction';
import { createSqsClient } from './sqs-client';

interface SqsWagerMessage {
  idempotencyKey: string;
  transaction: SubmitWagerCommand;
}

class PermanentSqsMessageError extends Error {}

@Injectable()
export class WagerTransactionConsumerService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(WagerTransactionConsumerService.name);
  private readonly sqsClient = createSqsClient();
  private readonly queueUrl =
    process.env.SQS_INPUT_QUEUE_URL ??
    'http://localhost:4566/000000000000/wager-transactions.fifo';
  private readonly deadLetterQueueUrl =
    process.env.SQS_DLQ_QUEUE_URL ??
    'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
  private stopped = false;
  private loopPromise: Promise<void> | undefined;
  private receiveController: AbortController | undefined;

  constructor(
    private readonly wageringService: WageringService,
    @Inject(MetricsService)
    private readonly metrics: MetricsService = new MetricsService(),
  ) {}

  onModuleInit(): void {
    this.loopPromise = this.run();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    this.receiveController?.abort();
    await this.loopPromise;
    this.sqsClient.destroy();
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      const receiveController = new AbortController();
      this.receiveController = receiveController;
      try {
        const response = await this.sqsClient.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 10,
            VisibilityTimeout: 45,
            MessageSystemAttributeNames: ['ApproximateReceiveCount'],
          }),
          { abortSignal: receiveController.signal },
        );
        this.receiveController = undefined;

        for (const message of response.Messages ?? []) {
          if (this.stopped) {
            if (message.ReceiptHandle !== undefined) {
              await this.sqsClient.send(
                new ChangeMessageVisibilityCommand({
                  QueueUrl: this.queueUrl,
                  ReceiptHandle: message.ReceiptHandle,
                  VisibilityTimeout: 0,
                }),
              );
            }
            continue;
          }
          await this.processMessage(message);
        }
        await this.refreshDeadLetterQueueDepth();
      } catch (error) {
        this.receiveController = undefined;
        if (this.stopped && receiveController.signal.aborted) {
          return;
        }
        this.logger.error(
          JSON.stringify({
            event: 'sqs_receive_failed',
            error: error instanceof Error ? error.message : String(error),
          }),
        );
        if (!this.stopped) {
          await this.delay(1000);
        }
      }
    }
  }

  private async refreshDeadLetterQueueDepth(): Promise<void> {
    const response = await this.sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.deadLetterQueueUrl,
        AttributeNames: [
          'ApproximateNumberOfMessages',
          'ApproximateNumberOfMessagesNotVisible',
        ],
      }),
    );
    this.metrics.setDeadLetterQueueDepth(
      Number(response.Attributes?.ApproximateNumberOfMessages ?? '0') +
        Number(
          response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0',
        ),
    );
  }

  private async processMessage(message: {
    MessageId?: string | undefined;
    ReceiptHandle?: string | undefined;
    Body?: string | undefined;
    Attributes?: Record<string, string> | undefined;
  }): Promise<void> {
    if (
      message.MessageId === undefined ||
      message.ReceiptHandle === undefined ||
      message.Body === undefined
    ) {
      this.logger.error(
        JSON.stringify({
          event: 'sqs_message_missing_attributes',
          correlationId: message.MessageId,
          messageId: message.MessageId,
        }),
      );
      return;
    }

    let envelope: SqsWagerMessage | undefined;
    let transactionId: string | undefined;
    try {
      envelope = this.parseMessage(message.Body);
      const payloadHash = createHash('sha256')
        .update(message.Body)
        .digest('hex');
      const result = await this.wageringService.submitWager(
        envelope.transaction,
        envelope.idempotencyKey,
        {
          consumerName: 'wager-transactions',
          messageId: message.MessageId,
          payloadHash,
          correlationId: message.MessageId,
        },
        false,
        message.MessageId,
      );
      transactionId = result.body.transactionId;
      await this.sqsClient.send(
        new DeleteMessageCommand({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
      this.logger.log(
        JSON.stringify({
          event: 'sqs_wager_processed',
          correlationId: message.MessageId,
          messageId: message.MessageId,
          transactionId,
          walletId: envelope.transaction.walletId,
          providerId: envelope.transaction.providerId,
        }),
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const context = {
        correlationId: message.MessageId,
        messageId: message.MessageId,
        ...(envelope === undefined
          ? {}
          : {
              ...(transactionId === undefined ? {} : { transactionId }),
              externalTransactionId:
                envelope.transaction.externalTransactionId,
              walletId: envelope.transaction.walletId,
              providerId: envelope.transaction.providerId,
            }),
        error: errorMessage,
      };
      if (error instanceof PermanentSqsMessageError) {
        await this.sqsClient.send(
          new SendMessageCommand({
            QueueUrl: this.deadLetterQueueUrl,
            MessageBody: JSON.stringify({
              sourceMessageId: message.MessageId,
              error: error.message,
              body: message.Body,
            }),
            MessageGroupId: 'invalid-wager-messages',
            MessageDeduplicationId: message.MessageId,
          }),
        );
        await this.sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          }),
        );
        this.metrics.recordDeadLetterMessage();
        this.logger.warn(
          JSON.stringify({ event: 'sqs_message_dead_lettered', ...context }),
        );
        return;
      }

      if (error instanceof HttpException && error.getStatus() < 500) {
        await this.sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          }),
        );
        this.logger.warn(
          JSON.stringify({
            event: 'sqs_message_rejected',
            statusCode: error.getStatus(),
            ...context,
          }),
        );
        return;
      }

      this.metrics.recordRetry('sqs');
      const receiveCount = Number.parseInt(
        message.Attributes?.ApproximateReceiveCount ?? '1',
        10,
      );
      const delaySeconds = Math.min(
        60,
        2 ** Math.min(Number.isNaN(receiveCount) ? 1 : receiveCount, 6),
      );
      await this.sqsClient.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: delaySeconds,
        }),
      );
      this.logger.error(
        JSON.stringify({
          event: 'sqs_message_retry_scheduled',
          visibilityTimeoutSeconds: delaySeconds,
          ...context,
        }),
      );
    }
  }

  private parseMessage(body: string): SqsWagerMessage {
    let value: unknown;
    try {
      value = JSON.parse(body);
    } catch {
      throw new PermanentSqsMessageError('SQS message body is not valid JSON');
    }
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new PermanentSqsMessageError('SQS message body must be an object');
    }

    const record = value as Record<string, unknown>;
    const data = record.data;
    const transactionValue =
      data === undefined ? record.transaction : data;
    const dataRecord =
      data !== null && typeof data === 'object' && !Array.isArray(data)
        ? (data as Record<string, unknown>)
        : undefined;
    if (
      data !== undefined &&
      record.type !== 'WagerTransactionRequested'
    ) {
      throw new PermanentSqsMessageError(
        'SQS event type must be WagerTransactionRequested',
      );
    }
    const idempotencyKey =
      record.idempotencyKey ?? dataRecord?.idempotencyKey;
    if (
      typeof idempotencyKey !== 'string' ||
      idempotencyKey.trim().length === 0
    ) {
      throw new PermanentSqsMessageError('SQS message is missing idempotencyKey');
    }
    const transaction = this.parseTransaction(transactionValue);
    return { idempotencyKey, transaction };
  }

  private parseTransaction(value: unknown): SubmitWagerCommand {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new PermanentSqsMessageError('SQS transaction must be an object');
    }
    const record = value as Record<string, unknown>;
    const kind = record.kind;
    const supportedKinds = [
      WagerTransactionKind.Bet,
      WagerTransactionKind.Win,
      WagerTransactionKind.Loss,
      WagerTransactionKind.Refund,
      WagerTransactionKind.Rollback,
    ];
    if (
      typeof kind !== 'string' ||
      !supportedKinds.includes(kind as SubmitWagerCommand['kind'])
    ) {
      throw new PermanentSqsMessageError('SQS transaction has an unsupported kind');
    }

    const money = record.money;
    if (money === null || typeof money !== 'object' || Array.isArray(money)) {
      throw new PermanentSqsMessageError('SQS transaction money must be an object');
    }
    const moneyRecord = money as Record<string, unknown>;
    const amount = this.requiredString(moneyRecord.amount, 'money.amount');
    const currency = this.requiredString(moneyRecord.currency, 'money.currency');
    const reference = record.referenceExternalTransactionId;
    if (reference !== undefined && typeof reference !== 'string') {
      throw new PermanentSqsMessageError('referenceExternalTransactionId must be a string');
    }

    return {
      providerId: this.requiredString(record.providerId, 'providerId'),
      externalTransactionId: this.requiredString(
        record.externalTransactionId,
        'externalTransactionId',
      ),
      playerId: this.requiredString(record.playerId, 'playerId'),
      walletId: this.requiredString(record.walletId, 'walletId'),
      roundId: this.requiredString(record.roundId, 'roundId'),
      gameId: this.requiredString(record.gameId, 'gameId'),
      kind: kind as SubmitWagerCommand['kind'],
      money: { amount, currency },
      ...(reference === undefined
        ? {}
        : { referenceExternalTransactionId: reference }),
    };
  }

  private requiredString(value: unknown, field: string): string {
    if (typeof value !== 'string' || value.trim().length === 0) {
      throw new PermanentSqsMessageError(`${field} must be a non-empty string`);
    }
    return value;
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
