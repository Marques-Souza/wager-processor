import { Money } from '../value-objects/money';
import { LedgerDirection } from './wallet-ledger-entry';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

export type FailureCode =
  | 'INSUFFICIENT_FUNDS'
  | 'REFERENCE_NOT_FOUND'
  | 'REFERENCE_MISMATCH'
  | 'REFERENCE_ALREADY_REVERSED'
  | 'REVERSAL_WOULD_OVERDRAW'
  | 'PERMANENT_INFRASTRUCTURE_ERROR';

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) { }


  static create(props: CreateWagerTransactionProps): WagerTransaction {
    const reference = props.referenceExternalTransactionId;
    const requiresReference =
      props.kind === WagerTransactionKind.Refund ||
      props.kind === WagerTransactionKind.Rollback;

    if (reference !== undefined && reference.trim().length === 0) {
      throw new TypeError('Reference transaction id must not be empty');
    }

    if (requiresReference && reference === undefined) {
      throw new TypeError('This transaction kind requires a reference');
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      reference,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt === undefined
      ? undefined
      : new Date(this._processedAt.getTime());
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      new Date(state.createdAt.getTime()),
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt === undefined
        ? undefined
        : new Date(state.processedAt.getTime()),
    );
  }

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  markPendingReference(): void {
    if (this._status !== WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(
        this._status,
        WagerTransactionStatus.PendingReference,
      );
    }

    if (
      this.referenceExternalTransactionId === undefined ||
      this.referenceExternalTransactionId.trim().length === 0
    ) {
      throw new TypeError('Cannot wait for a missing external reference id');
    }

    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(
        this._status,
        WagerTransactionStatus.Rejected,
      );
    }

    this._failureCode = code;
    this._status = WagerTransactionStatus.Rejected;
  }

  fail(code: FailureCode): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(
        this._status,
        WagerTransactionStatus.Failed,
      );
    }

    this._failureCode = code;
    this._status = WagerTransactionStatus.Failed;
  }

  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
  ): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(
        this._status,
        WagerTransactionStatus.Processed,
      );
    }

    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new TypeError('Processed date must be valid');
    }

    const requiresReference = this.requiresReference();
    const hasExternalReference =
      this.referenceExternalTransactionId !== undefined;

    if (
      referenceTransactionId !== undefined &&
      referenceTransactionId.trim().length === 0
    ) {
      throw new TypeError('Resolved reference id must not be empty');
    }

    if (
      (requiresReference || hasExternalReference) &&
      (referenceTransactionId === undefined ||
        referenceTransactionId.trim().length === 0)
    ) {
      throw new TypeError('Processed transaction requires a resolved reference');
    }

    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = new Date(at.getTime());
    this._status = WagerTransactionStatus.Processed;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;

      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;

      case WagerTransactionKind.Loss:
        throw new TypeError('LOSS does not produce a ledger entry');

      case WagerTransactionKind.Rollback:
        if (reference === undefined) {
          throw new TypeError('ROLLBACK requires its referenced transaction');
        }

        switch (reference.kind) {
          case WagerTransactionKind.Bet:
            return LedgerDirection.Credit;
          case WagerTransactionKind.Win:
          case WagerTransactionKind.Refund:
            return LedgerDirection.Debit;
          default:
            throw new TypeError('ROLLBACK reference kind is not supported');
        }

      default:
        throw new TypeError('Unsupported transaction kind');
    }
  }
}





export class InvalidTransactionStateError extends Error {
  constructor(
    currentStatus: WagerTransactionStatus,
    nextStatus: WagerTransactionStatus,
  ) {
    super(`Cannot transition transaction from ${currentStatus} to ${nextStatus}`);
    this.name = 'InvalidTransactionStateError';
  }


}


