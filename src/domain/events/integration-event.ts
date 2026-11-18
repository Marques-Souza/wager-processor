export interface IntegrationEventProps {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  occurredAt: Date;
  data: Readonly<Record<string, unknown>>;
}

export abstract class IntegrationEvent {
  readonly version = 1;

  protected constructor(
    public readonly eventId: string,
    public readonly aggregateId: string,
    public readonly correlationId: string,
    public readonly occurredAt: Date,
    public readonly data: Readonly<Record<string, unknown>>,
  ) {}

  abstract readonly eventType: string;

  toJSON(): Record<string, unknown> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

export class WagerTransactionProcessedEvent extends IntegrationEvent {
  readonly eventType = 'WagerTransactionProcessed';

  constructor(props: IntegrationEventProps) {
    super(
      props.eventId,
      props.aggregateId,
      props.correlationId,
      props.occurredAt,
      props.data,
    );
  }
}

export class WagerTransactionRejectedEvent extends IntegrationEvent {
  readonly eventType = 'WagerTransactionRejected';

  constructor(props: IntegrationEventProps) {
    super(
      props.eventId,
      props.aggregateId,
      props.correlationId,
      props.occurredAt,
      props.data,
    );
  }
}

export class WalletBalanceChangedEvent extends IntegrationEvent {
  readonly eventType = 'WalletBalanceChanged';

  constructor(props: IntegrationEventProps) {
    super(
      props.eventId,
      props.aggregateId,
      props.correlationId,
      props.occurredAt,
      props.data,
    );
  }
}

export class WagerTransactionPendingReferenceEvent extends IntegrationEvent {
  readonly eventType = 'WagerTransactionPendingReference';

  constructor(props: IntegrationEventProps) {
    super(
      props.eventId,
      props.aggregateId,
      props.correlationId,
      props.occurredAt,
      props.data,
    );
  }
}

export type IntegrationEventName =
  | 'WagerTransactionProcessed'
  | 'WagerTransactionRejected'
  | 'WalletBalanceChanged'
  | 'WagerTransactionPendingReference';

export function createIntegrationEvent(
  eventType: IntegrationEventName,
  props: IntegrationEventProps,
): IntegrationEvent {
  switch (eventType) {
    case 'WagerTransactionProcessed':
      return new WagerTransactionProcessedEvent(props);
    case 'WagerTransactionRejected':
      return new WagerTransactionRejectedEvent(props);
    case 'WalletBalanceChanged':
      return new WalletBalanceChangedEvent(props);
    case 'WagerTransactionPendingReference':
      return new WagerTransactionPendingReferenceEvent(props);
  }
}
