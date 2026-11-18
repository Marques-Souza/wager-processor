import { Money } from '../value-objects/money';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export type LedgerEntryState = CreateLedgerEntryProps;

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) { }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const {
      id,
      walletId,
      transactionId,
      direction,
      money,
      balanceBefore,
      balanceAfter,
      createdAt,
    } = props;

    if (!money.isPositive()) {
      throw new TypeError('Ledger amount must be positive');
    }

    if (balanceBefore.isNegative() || balanceAfter.isNegative()) {
      throw new TypeError('Ledger balances must not be negative');
    }

    if (!(createdAt instanceof Date) || Number.isNaN(createdAt.getTime())) {
      throw new TypeError('Ledger creation date must be valid');
    }

    let expectedBalance: Money;

    switch (direction) {
      case LedgerDirection.Credit:
        expectedBalance = balanceBefore.add(money);
        break;
      case LedgerDirection.Debit:
        expectedBalance = balanceBefore.subtract(money);
        break;
      default:
        throw new TypeError('Invalid ledger direction');
    }

    if (expectedBalance.isNegative() || !expectedBalance.equals(balanceAfter)) {
      throw new TypeError('Ledger entry does not balance');
    }

    return new WalletLedgerEntry(
      id,
      walletId,
      transactionId,
      direction,
      money,
      balanceBefore,
      balanceAfter,
      new Date(createdAt.getTime()),
    );
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      new Date(state.createdAt.getTime()),
    );
  }

  isBalanced(): boolean {
    if (
      !this.money.isPositive() ||
      this.balanceBefore.isNegative() ||
      this.balanceAfter.isNegative() ||
      this.money.currency !== this.balanceBefore.currency ||
      this.balanceBefore.currency !== this.balanceAfter.currency
    ) {
      return false;
    }

    switch (this.direction) {
      case LedgerDirection.Credit:
        return this.balanceBefore.add(this.money).equals(this.balanceAfter);
      case LedgerDirection.Debit:
        return this.balanceBefore.subtract(this.money).equals(this.balanceAfter);
      default:
        return false;
    }
  }
}