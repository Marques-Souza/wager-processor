import { describe, expect, it } from 'bun:test';
import { Money } from '../value-objects/money';
import { LedgerDirection } from './wallet-ledger-entry';
import {
  InvalidTransactionStateError,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction';

describe('WagerTransaction', () => {
  const validProps = {
    id: 'transaction-1',
    providerId: 'provider-1',
    externalTransactionId: 'external-1',
    idempotencyKey: 'key-1',
    payloadHash: 'hash-1',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerTransactionKind.Bet,
    money: Money.from({ amount: '10.00', currency: 'BRL' }),
    createdAt: new Date('2026-01-01T10:00:00.000Z'),
  };

  it('creates a BET in PENDING status', () => {
    const transaction = WagerTransaction.create({
      id: 'transaction-1',
      providerId: 'provider-1',
      externalTransactionId: 'external-1',
      idempotencyKey: 'key-1',
      payloadHash: 'hash-1',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '10.00', currency: 'BRL' }),
      createdAt: new Date(),
    });

    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });

  for (const kind of [
    WagerTransactionKind.Refund,
    WagerTransactionKind.Rollback,
  ]) {
    it(`requires a reference for ${kind}`, () => {
      expect(() =>
        WagerTransaction.create({
          id: 'transaction-1',
          providerId: 'provider-1',
          externalTransactionId: 'external-1',
          idempotencyKey: 'key-1',
          payloadHash: 'hash-1',
          walletId: 'wallet-1',
          playerId: 'player-1',
          roundId: 'round-1',
          gameId: 'game-1',
          kind,
          money: Money.from({ amount: '10.00', currency: 'BRL' }),
          createdAt: new Date(),
        }),
      ).toThrow(TypeError);
    });
  }

  it('rejects a blank reference', () => {
    expect(() =>
      WagerTransaction.create({
        id: 'transaction-1',
        providerId: 'provider-1',
        externalTransactionId: 'external-1',
        idempotencyKey: 'key-1',
        payloadHash: 'hash-1',
        walletId: 'wallet-1',
        playerId: 'player-1',
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: '10.00', currency: 'BRL' }),
        referenceExternalTransactionId: '   ',
        createdAt: new Date(),
      }),
    ).toThrow(TypeError);
  });

  it('rehydrates a rejected transaction with its persisted state', () => {
    const createdAt = new Date('2026-01-01T10:00:00.000Z');
    const processedAt = new Date('2026-01-01T10:01:00.000Z');

    const transaction = WagerTransaction.rehydrate({
      id: 'transaction-1',
      providerId: 'provider-1',
      externalTransactionId: 'external-1',
      idempotencyKey: 'key-1',
      payloadHash: 'hash-1',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '10.00', currency: 'BRL' }),
      createdAt,
      status: WagerTransactionStatus.Rejected,
      referenceTransactionId: 'internal-reference-1',
      failureCode: 'REFERENCE_MISMATCH',
      processedAt,
    });

    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.referenceTransactionId).toBe('internal-reference-1');
    expect(transaction.failureCode).toBe('REFERENCE_MISMATCH');
    expect(transaction.processedAt).toEqual(processedAt);
    expect(transaction.createdAt).toEqual(createdAt);
  });

  it('identifies terminal statuses', () => {
    const state = {
      id: 'transaction-1',
      providerId: 'provider-1',
      externalTransactionId: 'external-1',
      idempotencyKey: 'key-1',
      payloadHash: 'hash-1',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '10.00', currency: 'BRL' }),
      createdAt: new Date(),
    };

    expect(
      WagerTransaction.rehydrate({
        ...state,
        status: WagerTransactionStatus.Pending,
      }).isTerminal(),
    ).toBe(false);

    for (const status of [
      WagerTransactionStatus.Processed,
      WagerTransactionStatus.Rejected,
      WagerTransactionStatus.Failed,
    ]) {
      expect(
        WagerTransaction.rehydrate({ ...state, status }).isTerminal(),
      ).toBe(true);
    }
  });

  it('moves a transaction with a reference to PENDING_REFERENCE', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Win,
      referenceExternalTransactionId: 'external-reference-1',
    });

    transaction.markPendingReference();

    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
  });

  it('rejects PENDING_REFERENCE when no external reference exists', () => {
    const transaction = WagerTransaction.create(validProps);

    expect(() => transaction.markPendingReference()).toThrow(TypeError);
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });

  it('rejects PENDING_REFERENCE from a terminal status', () => {
    const transaction = WagerTransaction.rehydrate({
      ...validProps,
      referenceExternalTransactionId: 'external-reference-1',
      status: WagerTransactionStatus.Processed,
    });

    expect(() => transaction.markPendingReference()).toThrow(
      InvalidTransactionStateError,
    );
    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
  });

  it('rejects a pending transaction with a failure code', () => {
    const transaction = WagerTransaction.create(validProps);

    transaction.reject('INSUFFICIENT_FUNDS');

    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect(transaction.isTerminal()).toBe(true);
  });

  it('does not reject a transaction that is already terminal', () => {
    const transaction = WagerTransaction.rehydrate({
      ...validProps,
      status: WagerTransactionStatus.Processed,
    });

    expect(() => transaction.reject('REFERENCE_MISMATCH')).toThrow(
      InvalidTransactionStateError,
    );
    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.failureCode).toBeUndefined();
  });

  it('rejects a pending-reference transaction when its reference expires', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Win,
      referenceExternalTransactionId: 'external-reference-1',
    });

    transaction.markPendingReference();
    transaction.reject('REFERENCE_NOT_FOUND');

    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe('REFERENCE_NOT_FOUND');
    expect(transaction.isTerminal()).toBe(true);
  });

  it('marks a pending transaction as FAILED with a failure code', () => {
    const transaction = WagerTransaction.create(validProps);

    transaction.fail('PERMANENT_INFRASTRUCTURE_ERROR');

    expect(transaction.status).toBe(WagerTransactionStatus.Failed);
    expect(transaction.failureCode).toBe('PERMANENT_INFRASTRUCTURE_ERROR');
    expect(transaction.isTerminal()).toBe(true);
  });

  it('does not fail a transaction that is already terminal', () => {
    const transaction = WagerTransaction.rehydrate({
      ...validProps,
      status: WagerTransactionStatus.Rejected,
      failureCode: 'INSUFFICIENT_FUNDS',
    });

    expect(() =>
      transaction.fail('PERMANENT_INFRASTRUCTURE_ERROR'),
    ).toThrow(InvalidTransactionStateError);
    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe('INSUFFICIENT_FUNDS');
  });

  it('marks a BET as processed without a reference', () => {
    const transaction = WagerTransaction.create(validProps);
    const processedAt = new Date('2026-01-01T10:01:00.000Z');

    transaction.markProcessed(undefined, processedAt);

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.referenceTransactionId).toBeUndefined();
    expect(transaction.processedAt).toEqual(processedAt);
    expect(transaction.isTerminal()).toBe(true);
  });

  it('marks a referenced transaction as processed with its resolved id', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Win,
      referenceExternalTransactionId: 'external-reference-1',
    });
    const processedAt = new Date('2026-01-01T10:01:00.000Z');

    transaction.markProcessed('internal-reference-1', processedAt);

    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.referenceTransactionId).toBe('internal-reference-1');
    expect(transaction.processedAt).toEqual(processedAt);
  });

  it('rejects processing a refund without its resolved reference', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'external-reference-1',
    });

    expect(() =>
      transaction.markProcessed(undefined, new Date()),
    ).toThrow(TypeError);
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    expect(transaction.referenceTransactionId).toBeUndefined();
    expect(transaction.processedAt).toBeUndefined();
  });

  it('rejects a blank resolved reference id', () => {
    const transaction = WagerTransaction.create(validProps);

    expect(() =>
      transaction.markProcessed('   ', new Date()),
    ).toThrow(TypeError);
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
  });

  it('does not process a transaction that is already terminal', () => {
    const transaction = WagerTransaction.rehydrate({
      ...validProps,
      status: WagerTransactionStatus.Rejected,
      failureCode: 'INSUFFICIENT_FUNDS',
    });

    expect(() =>
      transaction.markProcessed(undefined, new Date()),
    ).toThrow(InvalidTransactionStateError);
    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect(transaction.processedAt).toBeUndefined();
  });

  it('requires references only for REFUND and ROLLBACK', () => {
    for (const kind of [
      WagerTransactionKind.Refund,
      WagerTransactionKind.Rollback,
    ]) {
      const transaction = WagerTransaction.create({
        ...validProps,
        kind,
        referenceExternalTransactionId: 'external-reference-1',
      });

      expect(transaction.requiresReference()).toBe(true);
    }

    for (const kind of [
      WagerTransactionKind.Opening,
      WagerTransactionKind.Bet,
      WagerTransactionKind.Win,
      WagerTransactionKind.Loss,
    ]) {
      const transaction = WagerTransaction.create({
        ...validProps,
        kind,
      });

      expect(transaction.requiresReference()).toBe(false);
    }
  });

  it('identifies which transaction kinds affect the balance', () => {
    const kinds = [
      [WagerTransactionKind.Opening, true],
      [WagerTransactionKind.Bet, true],
      [WagerTransactionKind.Win, true],
      [WagerTransactionKind.Loss, false],
      [WagerTransactionKind.Refund, true],
      [WagerTransactionKind.Rollback, true],
    ] as const;

    for (const [kind, expected] of kinds) {
      const transaction = WagerTransaction.create({
        ...validProps,
        kind,
        ...(kind === WagerTransactionKind.Refund ||
          kind === WagerTransactionKind.Rollback
          ? { referenceExternalTransactionId: 'external-reference-1' }
          : {}),
      });

      expect(transaction.affectsBalance()).toBe(expected);
    }
  });

  it('matches only the original payload hash', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      payloadHash: 'hash-1',
    });

    expect(transaction.matchesPayload('hash-1')).toBe(true);
    expect(transaction.matchesPayload('hash-different')).toBe(false);
  });

  it('returns the direction for transactions with a direct ledger effect', () => {
    const cases = [
      [WagerTransactionKind.Opening, LedgerDirection.Credit],
      [WagerTransactionKind.Bet, LedgerDirection.Debit],
      [WagerTransactionKind.Win, LedgerDirection.Credit],
      [WagerTransactionKind.Refund, LedgerDirection.Credit],
    ] as const;

    for (const [kind, expectedDirection] of cases) {
      const transaction = WagerTransaction.create({
        ...validProps,
        kind,
        ...(kind === WagerTransactionKind.Refund
          ? { referenceExternalTransactionId: 'external-reference-1' }
          : {}),
      });

      expect(transaction.ledgerDirectionFor()).toBe(expectedDirection);
    }
  });

  it('does not assign a ledger direction to LOSS', () => {
    const transaction = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Loss,
    });

    expect(() => transaction.ledgerDirectionFor()).toThrow(TypeError);
  });

  it('returns the inverse direction for ROLLBACK references', () => {
    const cases = [
      [WagerTransactionKind.Bet, LedgerDirection.Credit],
      [WagerTransactionKind.Win, LedgerDirection.Debit],
      [WagerTransactionKind.Refund, LedgerDirection.Debit],
    ] as const;

    const rollback = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'external-reference-1',
    });

    for (const [referenceKind, expectedDirection] of cases) {
      const reference = WagerTransaction.create({
        ...validProps,
        kind: referenceKind,
        ...(referenceKind === WagerTransactionKind.Refund
          ? { referenceExternalTransactionId: 'original-bet-1' }
          : {}),
      });

      expect(rollback.ledgerDirectionFor(reference)).toBe(expectedDirection);
    }
  });

  it('requires a supported reference to determine a ROLLBACK direction', () => {
    const rollback = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'external-reference-1',
    });
    const loss = WagerTransaction.create({
      ...validProps,
      kind: WagerTransactionKind.Loss,
    });

    expect(() => rollback.ledgerDirectionFor()).toThrow(TypeError);
    expect(() => rollback.ledgerDirectionFor(loss)).toThrow(TypeError);
  });

});