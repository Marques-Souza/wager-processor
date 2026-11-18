import { describe, expect, it } from 'bun:test';
import { MetricsService } from './metrics.service';
import { WagerTransactionStatus } from '../../domain/entities/wager-transaction';

describe('MetricsService', () => {
  it('renders financial, retry, lock, latency, and outbox metrics', () => {
    const metrics = new MetricsService();
    metrics.recordTransaction(WagerTransactionStatus.Processed);
    metrics.recordDuplicate('idempotency');
    metrics.recordRetry('sqs');
    metrics.recordDeadLetterMessage();
    metrics.setDeadLetterQueueDepth(3);
    metrics.recordLockConflict();
    metrics.observeProcessingDuration(0.25);
    metrics.setOutboxLagSeconds(2.5);

    const output = metrics.renderPrometheus();

    expect(output).toContain('wager_transactions_total{status="PROCESSED"} 1');
    expect(output).toContain('wager_duplicates_total{source="idempotency"} 1');
    expect(output).toContain('wager_retries_total{source="sqs"} 1');
    expect(output).toContain('wager_dlq_messages_total 1');
    expect(output).toContain('wager_dlq_queue_depth 3');
    expect(output).toContain('wager_lock_conflicts_total 1');
    expect(output).toContain('wager_outbox_lag_seconds 2.5');
    expect(output).toContain('wager_processing_duration_seconds_sum 0.25');
    expect(output).toContain('wager_processing_duration_seconds_count 1');
  });
});
