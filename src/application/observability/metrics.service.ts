import { Injectable } from '@nestjs/common';
import { WagerTransactionStatus } from '../../domain/entities/wager-transaction';

type DuplicateSource = 'idempotency' | 'inbox';
type RetrySource = 'sqs' | 'outbox' | 'pending_reference';

const STATUSES = Object.values(WagerTransactionStatus);

@Injectable()
export class MetricsService {
  private readonly transactions = new Map<WagerTransactionStatus, number>(
    STATUSES.map((status) => [status, 0]),
  );
  private readonly duplicates: Record<DuplicateSource, number> = {
    idempotency: 0,
    inbox: 0,
  };
  private readonly retries: Record<RetrySource, number> = {
    sqs: 0,
    outbox: 0,
    pending_reference: 0,
  };
  private deadLetterMessages = 0;
  private deadLetterQueueDepth = 0;
  private lockConflicts = 0;
  private outboxLagSeconds = 0;
  private processingDurationCount = 0;
  private processingDurationSum = 0;

  recordTransaction(status: WagerTransactionStatus): void {
    this.transactions.set(status, (this.transactions.get(status) ?? 0) + 1);
  }

  recordDuplicate(source: DuplicateSource): void {
    this.duplicates[source] += 1;
  }

  recordRetry(source: RetrySource): void {
    this.retries[source] += 1;
  }

  recordDeadLetterMessage(): void {
    this.deadLetterMessages += 1;
  }

  setDeadLetterQueueDepth(depth: number): void {
    this.deadLetterQueueDepth = Math.max(0, depth);
  }

  recordLockConflict(): void {
    this.lockConflicts += 1;
  }

  observeProcessingDuration(seconds: number): void {
    this.processingDurationCount += 1;
    this.processingDurationSum += Math.max(0, seconds);
  }

  setOutboxLagSeconds(seconds: number): void {
    this.outboxLagSeconds = Math.max(0, seconds);
  }

  renderPrometheus(): string {
    const lines = [
      '# HELP wager_transactions_total Wager transactions recorded by status.',
      '# TYPE wager_transactions_total counter',
      ...STATUSES.map(
        (status) =>
          `wager_transactions_total{status="${status}"} ${this.transactions.get(status) ?? 0}`,
      ),
      '# HELP wager_duplicates_total Duplicate submissions detected.',
      '# TYPE wager_duplicates_total counter',
      ...Object.entries(this.duplicates).map(
        ([source, count]) => `wager_duplicates_total{source="${source}"} ${count}`,
      ),
      '# HELP wager_retries_total Scheduled retries by component.',
      '# TYPE wager_retries_total counter',
      ...Object.entries(this.retries).map(
        ([source, count]) => `wager_retries_total{source="${source}"} ${count}`,
      ),
      '# HELP wager_dlq_messages_total Messages sent to the dead-letter queue.',
      '# TYPE wager_dlq_messages_total counter',
      `wager_dlq_messages_total ${this.deadLetterMessages}`,
      '# HELP wager_dlq_queue_depth Approximate number of messages currently in the DLQ.',
      '# TYPE wager_dlq_queue_depth gauge',
      `wager_dlq_queue_depth ${this.deadLetterQueueDepth}`,
      '# HELP wager_lock_conflicts_total Database lock conflicts.',
      '# TYPE wager_lock_conflicts_total counter',
      `wager_lock_conflicts_total ${this.lockConflicts}`,
      '# HELP wager_outbox_lag_seconds Age of the oldest pending outbox event.',
      '# TYPE wager_outbox_lag_seconds gauge',
      `wager_outbox_lag_seconds ${this.outboxLagSeconds}`,
      '# HELP wager_processing_duration_seconds Processing duration summary.',
      '# TYPE wager_processing_duration_seconds summary',
      `wager_processing_duration_seconds_sum ${this.processingDurationSum}`,
      `wager_processing_duration_seconds_count ${this.processingDurationCount}`,
    ];

    return `${lines.join('\n')}\n`;
  }
}
