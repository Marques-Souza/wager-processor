import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { EntityManager, type Transaction } from '@mikro-orm/core';
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { Inject } from '@nestjs/common';
import { MetricsService } from '../../application/observability/metrics.service';
import { createSqsClient } from './sqs-client';

interface OutboxRow {
  id: string;
  aggregate_id: string;
  payload: unknown;
}

@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private readonly sqsClient = createSqsClient();
  private readonly queueUrl =
    process.env.SQS_EVENTS_QUEUE_URL ??
    'http://localhost:4566/000000000000/wager-processor-events.fifo';
  private stopped = false;
  private loopPromise: Promise<void> | undefined;

  constructor(
    @Inject(EntityManager)
    private readonly entityManager: EntityManager,
    @Inject(MetricsService)
    private readonly metrics: MetricsService = new MetricsService(),
  ) {}

  onModuleInit(): void {
    this.loopPromise = this.run();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    await this.loopPromise;
    this.sqsClient.destroy();
  }

  async publishPendingOnce(): Promise<void> {
    await this.entityManager.getConnection().transactional(async (trx) => {
      const lagRow = (await this.entityManager.getConnection().execute(
        `        select coalesce(
          extract(
            epoch from now() - (
              min("occurred_at") filter (where "published_at" is null)
            )
          ),
          0
        )::text as lag_seconds
         from "outbox_messages"`,
        [],
        'get',
        trx,
      )) as { lag_seconds?: string } | undefined;
      this.metrics.setOutboxLagSeconds(
        Number.parseFloat(lagRow?.lag_seconds ?? '0'),
      );
      const rows = await this.query(
        `select "id", "aggregate_id", "payload"
         from "outbox_messages"
         where "published_at" is null
           and ("next_attempt_at" is null or "next_attempt_at" <= now())
         order by "occurred_at", "id"
         limit 10
         for update skip locked`,
        [],
        trx,
      );

      for (const row of rows) {
        await this.publishOne(row, trx);
      }
    });
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      try {
        await this.publishPendingOnce();
      } catch (error) {
        this.logger.error(
          'Outbox polling failed',
          error instanceof Error ? error.stack : String(error),
        );
      }

      if (!this.stopped) {
        await new Promise<void>((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async publishOne(row: OutboxRow, trx: Transaction): Promise<void> {
    try {
      await this.sqsClient.send(
        new SendMessageCommand({
          QueueUrl: this.queueUrl,
          MessageBody: JSON.stringify(row.payload),
          MessageGroupId: row.aggregate_id,
          MessageDeduplicationId: row.id,
        }),
      );
      await this.entityManager.getConnection().execute(
        `update "outbox_messages"
         set "published_at" = now(), "next_attempt_at" = null
         where "id" = ?`,
        [row.id],
        'run',
        trx,
      );
    } catch (error) {
      this.metrics.recordRetry('outbox');
      await this.entityManager.getConnection().execute(
        `update "outbox_messages"
         set "attempts" = "attempts" + 1,
             "next_attempt_at" = now()
               + make_interval(secs => least(power(2, least("attempts", 6)), 60))
         where "id" = ?`,
        [row.id],
        'run',
        trx,
      );
      this.logger.error(
        JSON.stringify({
          event: 'outbox_publish_retry_scheduled',
          eventId: row.id,
          aggregateId: row.aggregate_id,
          correlationId: this.correlationId(row.payload),
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  }

  private correlationId(payload: unknown): string | undefined {
    if (
      payload !== null &&
      typeof payload === 'object' &&
      'correlationId' in payload &&
      typeof payload.correlationId === 'string'
    ) {
      return payload.correlationId;
    }
    return undefined;
  }

  private async query(
    sql: string,
    params: unknown[],
    trx: Transaction,
  ): Promise<OutboxRow[]> {
    const result = await this.entityManager
      .getConnection()
      .execute(sql, params, 'all', trx);
    if (!Array.isArray(result)) {
      throw new Error('Outbox query did not return rows');
    }
    return result as OutboxRow[];
  }
}
