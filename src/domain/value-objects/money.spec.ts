import { describe, expect, it } from 'bun:test';
import { Money } from './money';

describe('Money.from', () => {
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
});