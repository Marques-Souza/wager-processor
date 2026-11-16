import Decimal from 'decimal.js';

export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ){}

    static from(props: MoneyProps): Money {
      const { amount, currency } = props;

      if (typeof amount !== 'string' || !/^\d+(?:\.\d{1,2})?$/.test(amount)){
        throw new TypeError('Invalid money amount');
      }

      if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)){
        throw new TypeError('Invalid currency');
      }
      return new Money(new Decimal(amount), currency);
    }
  
}