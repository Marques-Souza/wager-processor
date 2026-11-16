import { describe, expect, it } from 'bun:test';
import { CurrencyMismatchError, Money } from './money';

describe('Money', () => {
  it('creates Money with a valid amount and currency', () => {
    const money = Money.from({ amount: '25.00', currency: 'BRL' });

    expect(money).toBeInstanceOf(Money);
    expect(money.currency).toBe('BRL');
  });

  const invalidAmounts = ['', '-1.00', '1.001', '1e2', 'NaN', 'Infinity'];

  for (const amount of invalidAmounts) {
    it(`rejects invalid amount "${amount}"`, () => {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow(TypeError);
    });
  }

  it('rejects a currency that is not three uppercase letters', () => {
    expect(() => Money.from({ amount: '1.00', currency: 'brl' })).toThrow(TypeError);
  });

  it('creates zero Money with the requested currency', () => {
    const money = Money.zero('BRL');

    expect(money).toBeInstanceOf(Money);
    expect(money.currency).toBe('BRL');
  });

  it('rejects an invalid currency in the zero factory', () => {
    expect(() => Money.zero('brl')).toThrow(TypeError);
  });

  it('serializes an amount with two decimal places', () => {
    const money = Money.from({ amount: '25', currency: 'BRL' });

    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  it('serializes zero with two decimal places', () => {
    expect(Money.zero('BRL').toJSON()).toEqual({
      amount: '0.00',
      currency: 'BRL',
    });
  });

  it('detects zero and non-zero amounts', () => {
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(Money.from({ amount: '0.01', currency: 'BRL' }).isZero()).toBe(false);
  });

  it('detects positive amounts', () => {
    expect(Money.from({ amount: '0.01', currency: 'BRL' }).isPositive()).toBe(true);
    expect(Money.zero('BRL').isPositive()).toBe(false);
  });

  it('negates money without mutating the original', () => {
    const original = Money.from({ amount: '1.25', currency: 'BRL' });
    const negated = original.negate();

    expect(negated.toJSON()).toEqual({ amount: '-1.25', currency: 'BRL' });
    expect(original.toJSON()).toEqual({ amount: '1.25', currency: 'BRL' });
  });

  it('detects negative amounts and treats zero as non-negative', () => {
    const negative = Money.from({ amount: '1.25', currency: 'BRL' }).negate();

    expect(negative.isNegative()).toBe(true);
    expect(Money.zero('BRL').isNegative()).toBe(false);
  });

  it('adds decimal amounts exactly', () => {
    const result = Money.from({ amount: '0.10', currency: 'BRL' }).add(
      Money.from({ amount: '0.20', currency: 'BRL' }),
    );

    expect(result.toJSON()).toEqual({ amount: '0.30', currency: 'BRL' });
  });

  it('rejects adding amounts in different currencies', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
  });

  it('subtracts decimal amounts exactly', () => {
    const result = Money.from({ amount: '0.10', currency: 'BRL' }).subtract(
      Money.from({ amount: '0.30', currency: 'BRL' }),
    );

    expect(result.toJSON()).toEqual({ amount: '-0.20', currency: 'BRL' });
  });

  it('rejects subtracting amounts in different currencies', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
  });

  it('compares amounts in the same currency', () => {
    const one = Money.from({ amount: '1.00', currency: 'BRL' });
    const two = Money.from({ amount: '2.00', currency: 'BRL' });

    expect(one.isLessThan(two)).toBe(true);
    expect(two.isLessThan(one)).toBe(false);
    expect(one.isLessThan(one)).toBe(false);
  });

  it('rejects comparing amounts in different currencies', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '2.00', currency: 'USD' });

    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
  });

  it('compares amounts by value, not decimal scale', () => {
    const oneDecimal = Money.from({ amount: '1.0', currency: 'BRL' });
    const twoDecimals = Money.from({ amount: '1.00', currency: 'BRL' });
    const otherAmount = Money.from({ amount: '2.00', currency: 'BRL' });

    expect(oneDecimal.equals(twoDecimals)).toBe(true);
    expect(oneDecimal.equals(otherAmount)).toBe(false);
  });

  it('rejects comparing equality across different currencies', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
  });

  it('formats money as a readable string', () => {
    const money = Money.from({ amount: '25', currency: 'BRL' });

    expect(money.toString()).toBe('25.00 BRL');
  });
});