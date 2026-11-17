import { describe, expect, it } from 'bun:test';
import { CurrencyMismatchError, Money } from '../value-objects/money';
import {
  InsufficientWalletBalanceError,
  Wallet,
  type WalletState,
} from './wallet';

describe('Wallet', () => {
  const createdAt = new Date('2026-01-01T00:00:00.000Z');

  const walletState = (overrides: Partial<WalletState> = {}): WalletState => ({
    id: 'wallet-1',
    playerId: 'player-1',
    currency: 'BRL',
    balance: Money.from({ amount: '10.00', currency: 'BRL' }),
    version: 4,
    createdAt,
    updatedAt: createdAt,
    ...overrides,
  });

  it('opens with the initial balance and version one', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    expect(wallet.id).toBe('wallet-1');
    expect(wallet.playerId).toBe('player-1');
    expect(wallet.currency).toBe('BRL');
    expect(wallet.balance.toJSON()).toEqual({
      amount: '100.00',
      currency: 'BRL',
    });
    expect(wallet.version).toBe(1);
    expect(wallet.createdAt).toBeInstanceOf(Date);
    expect(wallet.updatedAt).toEqual(wallet.createdAt);
  });

  it('opens a zero-balance wallet at version one', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.zero('BRL'),
    });

    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
  });

  it('rejects empty identities when opening', () => {
    expect(() =>
      Wallet.open({
        id: ' ',
        playerId: 'player-1',
        initialBalance: Money.zero('BRL'),
      }),
    ).toThrow(TypeError);
    expect(() =>
      Wallet.open({
        id: 'wallet-1',
        playerId: '',
        initialBalance: Money.zero('BRL'),
      }),
    ).toThrow(TypeError);
  });

  it('rejects a negative initial balance', () => {
    expect(() =>
      Wallet.open({
        id: 'wallet-1',
        playerId: 'player-1',
        initialBalance: Money.from({ amount: '1.00', currency: 'BRL' }).negate(),
      }),
    ).toThrow(TypeError);
  });

  it('rehydrates a valid wallet state', () => {
    const wallet = Wallet.rehydrate(walletState());

    expect(wallet.balance.toJSON()).toEqual({
      amount: '10.00',
      currency: 'BRL',
    });
    expect(wallet.currency).toBe('BRL');
    expect(wallet.version).toBe(4);
    expect(wallet.createdAt).toEqual(createdAt);
    expect(wallet.updatedAt).toEqual(createdAt);
  });

  it('rejects invalid persisted state during rehydration', () => {
    expect(() =>
      Wallet.rehydrate(
        walletState({
          balance: Money.from({ amount: '1.00', currency: 'BRL' }).negate(),
        }),
      ),
    ).toThrow(TypeError);

    expect(() =>
      Wallet.rehydrate(walletState({ version: 0 })),
    ).toThrow(TypeError);

    expect(() =>
      Wallet.rehydrate(walletState({ currency: 'USD' })),
    ).toThrow(CurrencyMismatchError);

    expect(() =>
      Wallet.rehydrate(walletState({ updatedAt: new Date(Number.NaN) })),
    ).toThrow(TypeError);
  });

  it('credits the balance and returns the change for ledger creation', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '10.00', currency: 'BRL' }),
    });
    const occurredAt = new Date('2026-01-02T00:00:00.000Z');

    const change = wallet.credit(
      Money.from({ amount: '2.50', currency: 'BRL' }),
      occurredAt,
    );

    expect(change).toEqual({
      direction: 'CREDIT',
      money: Money.from({ amount: '2.50', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '10.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '12.50', currency: 'BRL' }),
      walletVersion: 2,
      occurredAt,
    });
    expect(wallet.balance.toJSON()).toEqual({
      amount: '12.50',
      currency: 'BRL',
    });
    expect(wallet.version).toBe(2);
    expect(wallet.updatedAt).toEqual(occurredAt);
  });

  it('debits the balance and returns the change for ledger creation', () => {
    const wallet = Wallet.rehydrate(walletState({
      id: 'wallet-1',
      playerId: 'player-1',
      balance: Money.from({ amount: '10.00', currency: 'BRL' }),
      currency: 'BRL',
      version: 2,
    }));
    const occurredAt = new Date('2026-01-02T00:00:00.000Z');

    const change = wallet.debit(
      Money.from({ amount: '3.50', currency: 'BRL' }),
      occurredAt,
    );

    expect(change).toEqual({
      direction: 'DEBIT',
      money: Money.from({ amount: '3.50', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '10.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '6.50', currency: 'BRL' }),
      walletVersion: 3,
      occurredAt,
    });
    expect(wallet.balance.toJSON()).toEqual({
      amount: '6.50',
      currency: 'BRL',
    });
    expect(wallet.version).toBe(3);
    expect(wallet.updatedAt).toEqual(occurredAt);
  });

  it('does not change balance, version, or updatedAt for zero amounts', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '10.00', currency: 'BRL' }),
    });
    const updatedAtBefore = wallet.updatedAt;
    const occurredAt = new Date('2026-01-02T00:00:00.000Z');

    expect(wallet.credit(Money.zero('BRL'), occurredAt)).toBeUndefined();
    expect(wallet.debit(Money.zero('BRL'), occurredAt)).toBeUndefined();

    expect(wallet.balance.toJSON()).toEqual({
      amount: '10.00',
      currency: 'BRL',
    });
    expect(wallet.version).toBe(1);
    expect(wallet.updatedAt).toEqual(updatedAtBefore);
  });

  it('rejects an overdraft without changing balance or version', () => {
    const wallet = Wallet.rehydrate(
      walletState({
        balance: Money.from({ amount: '5.00', currency: 'BRL' }),
        version: 3,
      }),
    );
    const updatedAtBefore = wallet.updatedAt;

    expect(() =>
      wallet.debit(
        Money.from({ amount: '5.01', currency: 'BRL' }),
        new Date('2026-01-02T00:00:00.000Z'),
      ),
    ).toThrow(InsufficientWalletBalanceError);
    expect(wallet.balance.toJSON()).toEqual({ amount: '5.00', currency: 'BRL' });
    expect(wallet.version).toBe(3);
    expect(wallet.updatedAt).toEqual(updatedAtBefore);
  });

  it('rejects negative amounts without changing balance or version', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.zero('BRL'),
    });
    const negativeAmount = Money.from({ amount: '1.00', currency: 'BRL' }).negate();
    const occurredAt = new Date('2026-01-02T00:00:00.000Z');

    expect(() => wallet.credit(negativeAmount, occurredAt)).toThrow(TypeError);
    expect(() => wallet.debit(negativeAmount, occurredAt)).toThrow(TypeError);
    expect(wallet.balance.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);
  });

  it('rejects a currency mismatch without changing state', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.zero('BRL'),
    });
    const occurredAt = new Date('2026-01-02T00:00:00.000Z');

    expect(() =>
      wallet.credit(
        Money.from({ amount: '1.00', currency: 'USD' }),
        occurredAt,
      ),
    ).toThrow(CurrencyMismatchError);
    expect(wallet.balance.toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);
  });
});
