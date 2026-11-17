import { CurrencyMismatchError, Money } from '../value-objects/money';

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
}

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export type WalletBalanceDirection = 'CREDIT' | 'DEBIT';

export interface WalletBalanceChange {
  direction: WalletBalanceDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  walletVersion: number;
  occurredAt: Date;
}

export class InsufficientWalletBalanceError extends Error {
  constructor() {
    super('Wallet balance is insufficient');
    this.name = 'InsufficientWalletBalanceError';
  }
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): Wallet {
    Wallet.assertIdentity(props.id, props.playerId);
    Wallet.assertMoney(props.initialBalance);

    if (props.initialBalance.isNegative()) {
      throw new TypeError('Wallet initial balance must not be negative');
    }

    const now = new Date();

    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      new Date(now.getTime()),
    );
  }

  static rehydrate(state: WalletState): Wallet {
    Wallet.assertIdentity(state.id, state.playerId);
    Wallet.assertMoney(state.balance);

    if (state.currency !== state.balance.currency) {
      throw new CurrencyMismatchError();
    }

    if (state.balance.isNegative()) {
      throw new TypeError('Wallet balance must not be negative');
    }

    if (!Number.isSafeInteger(state.version) || state.version < 1) {
      throw new TypeError('Wallet version must be a positive safe integer');
    }

    Wallet.assertDate(state.createdAt, 'createdAt');
    Wallet.assertDate(state.updatedAt, 'updatedAt');

    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      new Date(state.createdAt.getTime()),
      new Date(state.updatedAt.getTime()),
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return new Date(this._updatedAt.getTime());
  }

  credit(amount: Money, occurredAt: Date): WalletBalanceChange | undefined {
    this.assertSameCurrency(amount);
    Wallet.assertDate(occurredAt, 'occurredAt');

    if (amount.isNegative()) {
      throw new TypeError('Wallet credit must not be negative');
    }

    if (amount.isZero()) {
      return undefined;
    }

    const balanceBefore = this._balance;
    const balanceAfter = balanceBefore.add(amount);

    return this.recordBalanceChange(
      'CREDIT',
      amount,
      balanceBefore,
      balanceAfter,
      occurredAt,
    );
  }

  debit(amount: Money, occurredAt: Date): WalletBalanceChange | undefined {
    this.assertSameCurrency(amount);
    Wallet.assertDate(occurredAt, 'occurredAt');

    if (amount.isNegative()) {
      throw new TypeError('Wallet debit must not be negative');
    }

    if (amount.isZero()) {
      return undefined;
    }

    const balanceBefore = this._balance;
    const balanceAfter = balanceBefore.subtract(amount);

    if (balanceAfter.isNegative()) {
      throw new InsufficientWalletBalanceError();
    }

    return this.recordBalanceChange(
      'DEBIT',
      amount,
      balanceBefore,
      balanceAfter,
      occurredAt,
    );
  }

  private assertSameCurrency(money: Money): void {
    Wallet.assertMoney(money);

    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError();
    }
  }

  private recordBalanceChange(
    direction: WalletBalanceDirection,
    money: Money,
    balanceBefore: Money,
    balanceAfter: Money,
    occurredAt: Date,
  ): WalletBalanceChange | undefined {
    if (balanceAfter.equals(balanceBefore)) {
      return undefined;
    }

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = new Date(occurredAt.getTime());

    return {
      direction,
      money,
      balanceBefore,
      balanceAfter,
      walletVersion: this._version,
      occurredAt: new Date(occurredAt.getTime()),
    };
  }

  private static assertIdentity(id: string, playerId: string): void {
    if (typeof id !== 'string' || id.trim().length === 0) {
      throw new TypeError('Wallet id must not be empty');
    }

    if (typeof playerId !== 'string' || playerId.trim().length === 0) {
      throw new TypeError('Wallet playerId must not be empty');
    }
  }

  private static assertMoney(money: Money): void {
    if (!(money instanceof Money)) {
      throw new TypeError('Wallet values must be Money instances');
    }
  }

  private static assertDate(date: Date, name: string): void {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
      throw new TypeError(`Wallet ${name} must be a valid Date`);
    }
  }
}
