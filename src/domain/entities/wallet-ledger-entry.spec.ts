import { describe, expect, it } from 'bun:test';
import { CurrencyMismatchError, Money } from '../value-objects/money';
import {
  CreateLedgerEntryProps,
  LedgerDirection,
  WalletLedgerEntry,
} from './wallet-ledger-entry';

describe('WalletLedgerEntry', () => {
  const createProps = (
    overrides: Partial<CreateLedgerEntryProps> = {},
  ): CreateLedgerEntryProps => ({
    id: 'ledger-1',
    walletId: 'wallet-1',
    transactionId: 'transaction-1',
    direction: LedgerDirection.Credit,
    money: Money.from({ amount: '2.50', currency: 'BRL' }),
    balanceBefore: Money.from({ amount: '10.00', currency: 'BRL' }),
    balanceAfter: Money.from({ amount: '12.50', currency: 'BRL' }),
    createdAt: new Date('2026-01-01T10:00:00.000Z'),
    ...overrides,
  });

  it('creates a balanced credit entry', () => {
    const entry = WalletLedgerEntry.create(createProps());

    expect(entry.direction).toBe(LedgerDirection.Credit);
    expect(entry.balanceBefore.toJSON()).toEqual({
      amount: '10.00',
      currency: 'BRL',
    });
    expect(entry.balanceAfter.toJSON()).toEqual({
      amount: '12.50',
      currency: 'BRL',
    });
  });

  it('creates a balanced debit entry', () => {
    const entry = WalletLedgerEntry.create(
      createProps({
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '3.50', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '10.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '6.50', currency: 'BRL' }),
      }),
    );

    expect(entry.direction).toBe(LedgerDirection.Debit);
    expect(entry.balanceAfter.toJSON()).toEqual({
      amount: '6.50',
      currency: 'BRL',
    });
  });

  it('rejects a balance that does not match the entry direction and amount', () => {
    expect(() =>
      WalletLedgerEntry.create(
        createProps({
          balanceAfter: Money.from({ amount: '13.00', currency: 'BRL' }),
        }),
      ),
    ).toThrow(TypeError);
  });

  it('rejects a debit that would result in a negative balance', () => {
    expect(() =>
      WalletLedgerEntry.create(
        createProps({
          direction: LedgerDirection.Debit,
          money: Money.from({ amount: '6.00', currency: 'BRL' }),
          balanceBefore: Money.from({ amount: '5.00', currency: 'BRL' }),
          balanceAfter: Money.zero('BRL'),
        }),
      ),
    ).toThrow(TypeError);
  });

  it('rejects zero and negative entry amounts', () => {
    expect(() =>
      WalletLedgerEntry.create(
        createProps({
          money: Money.zero('BRL'),
          balanceAfter: Money.from({ amount: '10.00', currency: 'BRL' }),
        }),
      ),
    ).toThrow(TypeError);

    expect(() =>
      WalletLedgerEntry.create(
        createProps({
          money: Money.from({ amount: '1.00', currency: 'BRL' }).negate(),
          balanceAfter: Money.from({ amount: '9.00', currency: 'BRL' }),
        }),
      ),
    ).toThrow(TypeError);
  });

  it('rejects different currencies in the ledger calculation', () => {
    expect(() =>
      WalletLedgerEntry.create(
        createProps({
          money: Money.from({ amount: '2.50', currency: 'USD' }),
        }),
      ),
    ).toThrow(CurrencyMismatchError);
  });

  it('rejects an invalid creation date', () => {
    expect(() =>
      WalletLedgerEntry.create(
        createProps({ createdAt: new Date(Number.NaN) }),
      ),
    ).toThrow(TypeError);
  });

  it('rehydrates a ledger entry and reports its balance is correct', () => {
    const state = createProps();
    const entry = WalletLedgerEntry.rehydrate(state);

    expect(entry.id).toBe(state.id);
    expect(entry.walletId).toBe(state.walletId);
    expect(entry.transactionId).toBe(state.transactionId);
    expect(entry.createdAt).toEqual(state.createdAt);
    expect(entry.isBalanced()).toBe(true);
  });

  it('reports an inconsistent rehydrated entry as unbalanced', () => {
    const entry = WalletLedgerEntry.rehydrate(
      createProps({
        balanceAfter: Money.from({ amount: '13.00', currency: 'BRL' }),
      }),
    );

    expect(entry.isBalanced()).toBe(false);
  });

  it('reports rehydrated entries with inconsistent currencies as unbalanced', () => {
    const entry = WalletLedgerEntry.rehydrate(
      createProps({
        balanceAfter: Money.from({ amount: '12.50', currency: 'USD' }),
      }),
    );

    expect(entry.isBalanced()).toBe(false);
  });
});
