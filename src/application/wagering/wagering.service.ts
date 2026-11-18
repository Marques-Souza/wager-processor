import { createHash, randomUUID } from 'node:crypto';
import {
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
  UnprocessableEntityException,
} from '@nestjs/common';
import { EntityManager, type Transaction } from '@mikro-orm/core';
import {
  InsufficientWalletBalanceError,
  Wallet,
} from '../../domain/aggregates/wallet';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../domain/entities/wallet-ledger-entry';
import {
  FailureCode,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/entities/wager-transaction';
import { Money, MoneyProps } from '../../domain/value-objects/money';
import {
  createIntegrationEvent,
  IntegrationEventName,
} from '../../domain/events/integration-event';
import { MetricsService } from '../observability/metrics.service';

type DbRow = Record<string, unknown>;

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface SubmitWagerCommand {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<WagerTransactionKind, WagerTransactionKind.Opening>;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface InboxContext {
  consumerName: string;
  messageId: string;
  payloadHash: string;
  correlationId?: string;
}

interface WagerResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
}

interface WagerHttpResult {
  httpStatus: 200 | 202 | 422;
  body: WagerResult;
}

@Injectable()
export class WageringService {
  private readonly logger = new Logger(WageringService.name);

  constructor(
    @Inject(EntityManager)
    private readonly entityManager: EntityManager,
    @Inject(MetricsService)
    private readonly metrics: MetricsService = new MetricsService(),
  ) {}

  async createWallet(command: CreateWalletCommand): Promise<{
    id: string;
    playerId: string;
    balance: MoneyProps;
    version: number;
  }> {
    this.assertLength(command.playerId, 'playerId', 200);
    const initialBalance = this.parseMoney(command.initialBalance);
    if (initialBalance.isNegative()) {
      throw new UnprocessableEntityException('Initial balance cannot be negative');
    }

    const walletId = randomUUID();
    const now = new Date();
    const wallet = Wallet.open({
      id: walletId,
      playerId: command.playerId,
      initialBalance,
    });

    try {
      await this.transaction(async (trx) => {
        await this.query(
          `insert into "wallets"
            ("id", "player_id", "currency", "balance", "version", "created_at", "updated_at")
           values (?, ?, ?, ?, ?, ?, ?)`,
          [
            wallet.id,
            wallet.playerId,
            wallet.currency,
            initialBalance.toJSON().amount,
            wallet.version,
            wallet.createdAt,
            wallet.updatedAt,
          ],
          trx,
        );

        if (initialBalance.isPositive()) {
          const transactionId = randomUUID();
          const ledgerId = randomUUID();
          const openingKey = `opening:${wallet.id}`;
          const openingHash = this.hashPayload({
            playerId: wallet.playerId,
            walletId: wallet.id,
            kind: WagerTransactionKind.Opening,
            money: initialBalance.toJSON(),
          });
          const opening = WagerTransaction.create({
            id: transactionId,
            providerId: 'internal',
            externalTransactionId: openingKey,
            idempotencyKey: openingKey,
            payloadHash: openingHash,
            walletId: wallet.id,
            playerId: wallet.playerId,
            roundId: 'wallet-opening',
            gameId: 'internal',
            kind: WagerTransactionKind.Opening,
            money: initialBalance,
            createdAt: now,
          });
          opening.markProcessed(undefined, now);

          await this.query(
            `insert into "wager_transactions"
              ("id", "provider_id", "external_transaction_id", "idempotency_key",
               "payload_hash", "wallet_id", "player_id", "currency", "round_id",
               "game_id", "kind", "status", "amount", "created_at", "processed_at",
               "result_balance", "result_currency")
             values (?, 'internal', ?, ?, ?, ?, ?, ?, 'wallet-opening',
               'internal', 'OPENING', 'PROCESSED', ?, ?, ?, ?, ?)`,
            [
              opening.id,
              opening.externalTransactionId,
               opening.externalTransactionId,
              opening.payloadHash,
              wallet.id,
              wallet.playerId,
              wallet.currency,
              initialBalance.toJSON().amount,
              now,
               now,
               initialBalance.toJSON().amount,
               wallet.currency,
            ],
            trx,
          );

          const entry = WalletLedgerEntry.create({
            id: ledgerId,
            walletId: wallet.id,
            transactionId: opening.id,
            direction: opening.ledgerDirectionFor(),
            money: initialBalance,
            balanceBefore: Money.zero(wallet.currency),
            balanceAfter: initialBalance,
            createdAt: now,
          });
          await this.insertLedger(entry, trx);
          await this.insertOutbox(
            'WagerTransactionProcessed',
            wallet.id,
            {
              transactionId: opening.id,
              walletId: wallet.id,
              kind: opening.kind,
              status: opening.status,
            },
            now,
            trx,
          );
          await this.insertOutbox(
            'WalletBalanceChanged',
            wallet.id,
            {
              transactionId: opening.id,
              walletId: wallet.id,
              direction: entry.direction,
              money: entry.money.toJSON(),
              balanceBefore: entry.balanceBefore.toJSON(),
              balanceAfter: entry.balanceAfter.toJSON(),
              walletVersion: wallet.version,
            },
            now,
            trx,
          );
        }
      });
    } catch (error) {
      this.rethrowDatabaseError(error);
    }

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: initialBalance.toJSON(),
      version: wallet.version,
    };
  }

  async submitWager(
    command: SubmitWagerCommand,
    idempotencyKey: string,
    inbox?: InboxContext,
    retryPendingReference = false,
    correlationId?: string,
  ): Promise<WagerHttpResult> {
    const startedAt = performance.now();
    this.assertLength(idempotencyKey, 'Idempotency-Key', 300);
    this.validateSubmitCommand(command);

    const money = this.parseMoney(command.money);
    if (!money.isPositive()) {
      throw new UnprocessableEntityException('Transaction amount must be positive');
    }

    const createdAt = new Date();
    const payloadHash = this.hashPayload({
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      playerId: command.playerId,
      walletId: command.walletId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money: money.toJSON(),
      ...(command.referenceExternalTransactionId === undefined
        ? {}
        : { referenceExternalTransactionId: command.referenceExternalTransactionId }),
    });
    let transaction = WagerTransaction.create({
      id: randomUUID(),
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      idempotencyKey,
      payloadHash,
      walletId: command.walletId,
      playerId: command.playerId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money,
      ...(command.referenceExternalTransactionId === undefined
        ? {}
        : {
            referenceExternalTransactionId:
              command.referenceExternalTransactionId,
          }),
      createdAt,
    });

    let isNewWager = false;
    try {
      return await this.transaction(async (trx) => {
        await this.query(
          'select pg_advisory_xact_lock(hashtextextended(?, 0))',
          [idempotencyKey],
          trx,
        );

        let pendingResultBalance: Money | undefined;
        const complete = async (
          result: WagerHttpResult,
        ): Promise<WagerHttpResult> => {
          if (
            isNewWager ||
            (retryPendingReference &&
              result.body.status !== WagerTransactionStatus.PendingReference)
          ) {
            this.metrics.recordTransaction(result.body.status);
          }
          if (inbox !== undefined) {
            await this.query(
              `update "inbox_messages" set "processed_at" = now()
               where "consumer_name" = ? and "message_id" = ?`,
              [inbox.consumerName, inbox.messageId],
              trx,
            );
          }
          return result;
        };

        if (inbox !== undefined) {
          this.assertNonEmpty(inbox.consumerName, 'consumerName');
          this.assertNonEmpty(inbox.messageId, 'messageId');
          const received = await this.query(
            `insert into "inbox_messages"
              ("consumer_name", "message_id", "payload_hash", "received_at")
             values (?, ?, ?, now())
             on conflict ("consumer_name", "message_id") do nothing
             returning "message_id"`,
            [inbox.consumerName, inbox.messageId, inbox.payloadHash],
            trx,
          );
          if (received.length === 0) {
            this.metrics.recordDuplicate('inbox');
            const existingInboxRows = await this.query(
              `select "payload_hash", "processed_at" from "inbox_messages"
               where "consumer_name" = ? and "message_id" = ?`,
              [inbox.consumerName, inbox.messageId],
              trx,
            );
            const existingInbox = existingInboxRows[0];
            if (
              existingInbox === undefined ||
              existingInbox.payload_hash !== inbox.payloadHash
            ) {
              throw new ConflictException(
                'SQS message id was reused with a different payload',
              );
            }
            if (existingInbox.processed_at === null) {
              throw new Error('Inbox message was committed before processing completed');
            }
            const existingWagerRows = await this.query(
              `select "id", "status", "result_balance", "result_currency"
               from "wager_transactions" where "idempotency_key" = ?`,
              [idempotencyKey],
              trx,
            );
            const existingWager = existingWagerRows[0];
            if (existingWager === undefined) {
              throw new Error('Processed inbox message has no persisted wager');
            }
            return this.persistedWagerResult(existingWager);
          }
        }

        const inserted = await this.query(
          `insert into "wager_transactions"
            ("id", "provider_id", "external_transaction_id", "idempotency_key",
             "payload_hash", "wallet_id", "player_id", "currency", "round_id",
             "game_id", "kind", "status", "amount",
             "reference_external_transaction_id", "created_at")
           values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
           on conflict ("idempotency_key") do nothing
           returning "id"`,
          [
            transaction.id,
            transaction.providerId,
            transaction.externalTransactionId,
            transaction.idempotencyKey,
            transaction.payloadHash,
            transaction.walletId,
            transaction.playerId,
            money.currency,
            transaction.roundId,
            transaction.gameId,
            transaction.kind,
            money.toJSON().amount,
            transaction.referenceExternalTransactionId ?? null,
            createdAt,
          ],
          trx,
        );

        if (inserted.length === 0) {
          this.metrics.recordDuplicate('idempotency');
          const existingRows = await this.query(
            `select * from "wager_transactions"
             where "idempotency_key" = ? for update`,
            [idempotencyKey],
            trx,
          );
          const existing = existingRows[0];
          if (existing === undefined) {
            throw new Error('Idempotency conflict row could not be loaded');
          }
          if (existing.payload_hash !== payloadHash) {
            throw new ConflictException(
              'Idempotency-Key was already used with a different payload',
            );
          }
          if (
            retryPendingReference &&
            existing.status === WagerTransactionStatus.PendingReference
          ) {
            transaction = this.rehydrateTransaction(existing);
            if (
              typeof existing.result_balance !== 'string' ||
              typeof existing.result_currency !== 'string'
            ) {
              throw new Error('Pending wager has no persisted result balance');
            }
            pendingResultBalance = Money.from({
              amount: existing.result_balance,
              currency: existing.result_currency,
            });
          } else {
            return complete(this.persistedWagerResult(existing));
          }
        }
        isNewWager = inserted.length > 0;

        const walletRows = await this.query(
          `select "id", "player_id", "currency", "balance", "version",
                  "created_at", "updated_at"
           from "wallets" where "id" = ? for update`,
          [transaction.walletId],
          trx,
        );
        const walletRow = walletRows[0];
        if (walletRow === undefined) {
          throw new NotFoundException('Wallet not found');
        }
        if (walletRow.player_id !== transaction.playerId) {
          throw new UnprocessableEntityException('Wallet does not belong to player');
        }
        if (walletRow.currency !== money.currency) {
          throw new UnprocessableEntityException('Wallet currency does not match transaction');
        }

        const wallet = Wallet.rehydrate({
          id: String(walletRow.id),
          playerId: String(walletRow.player_id),
          currency: String(walletRow.currency),
          balance: Money.from({
            amount: this.databaseDecimal(walletRow.balance),
            currency: String(walletRow.currency),
          }),
          version: Number(walletRow.version),
          createdAt: this.databaseDate(walletRow.created_at),
          updatedAt: this.databaseDate(walletRow.updated_at),
        });

        let reference: WagerTransaction | undefined;
        if (transaction.referenceExternalTransactionId !== undefined) {
          const referenceRows = await this.query(
            `select * from "wager_transactions"
             where "provider_id" = ? and "external_transaction_id" = ?`,
            [transaction.providerId, transaction.referenceExternalTransactionId],
            trx,
          );
          const referenceRow = referenceRows[0];
          if (referenceRow === undefined) {
            if (transaction.status === WagerTransactionStatus.Pending) {
              transaction.markPendingReference();
            }
            const responseBalance = pendingResultBalance ?? wallet.balance;
            await this.persistWagerStatus(
              transaction,
              responseBalance,
              undefined,
              trx,
            );
            await this.schedulePendingReference(transaction.id, trx);
            if (isNewWager) {
              await this.insertOutbox(
                'WagerTransactionPendingReference',
                wallet.id,
                {
                  transactionId: transaction.id,
                  walletId: wallet.id,
                  providerId: transaction.providerId,
                  referenceExternalTransactionId:
                    transaction.referenceExternalTransactionId,
                },
                createdAt,
                trx,
                correlationId,
              );
            }
            return complete({
              httpStatus: 202,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: responseBalance.toJSON(),
                idempotentReplay: false,
              },
            });
          }

          reference = this.rehydrateTransaction(referenceRow);
          if (!this.referenceMatches(transaction, reference)) {
            transaction.reject('REFERENCE_MISMATCH');
            await this.persistWagerStatus(
              transaction,
              wallet.balance,
              reference.id,
              trx,
            );
            await this.insertRejectedEvent(
              transaction,
              wallet.id,
              createdAt,
              trx,
              correlationId,
            );
            return complete({
              httpStatus: 422,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: wallet.balance.toJSON(),
                idempotentReplay: false,
              },
            });
          }

          if (
            reference.status === WagerTransactionStatus.Pending ||
            reference.status === WagerTransactionStatus.PendingReference
          ) {
            if (transaction.status === WagerTransactionStatus.Pending) {
              transaction.markPendingReference();
            }
            const responseBalance = pendingResultBalance ?? wallet.balance;
            await this.persistWagerStatus(
              transaction,
              responseBalance,
              reference.id,
              trx,
            );
            await this.schedulePendingReference(transaction.id, trx);
            if (isNewWager) {
              await this.insertOutbox(
                'WagerTransactionPendingReference',
                wallet.id,
                {
                  transactionId: transaction.id,
                  walletId: wallet.id,
                  providerId: transaction.providerId,
                  referenceExternalTransactionId:
                    transaction.referenceExternalTransactionId,
                },
                createdAt,
                trx,
                correlationId,
              );
            }
            return complete({
              httpStatus: 202,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: responseBalance.toJSON(),
                idempotentReplay: false,
              },
            });
          }

          if (
            reference.status !== WagerTransactionStatus.Processed ||
            !this.referenceKindAllowed(transaction, reference)
          ) {
            transaction.reject('REFERENCE_MISMATCH');
            await this.persistWagerStatus(
              transaction,
              wallet.balance,
              reference.id,
              trx,
            );
            await this.insertRejectedEvent(
              transaction,
              wallet.id,
              createdAt,
              trx,
              correlationId,
            );
            return complete({
              httpStatus: 422,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: wallet.balance.toJSON(),
                idempotentReplay: false,
              },
            });
          }

          if (
            transaction.requiresReference() &&
            !transaction.money.equals(reference.money)
          ) {
            transaction.reject('REFERENCE_MISMATCH');
            await this.persistWagerStatus(
              transaction,
              wallet.balance,
              reference.id,
              trx,
            );
            await this.insertRejectedEvent(
              transaction,
              wallet.id,
              createdAt,
              trx,
              correlationId,
            );
            return complete({
              httpStatus: 422,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: wallet.balance.toJSON(),
                idempotentReplay: false,
              },
            });
          }

          if (
            transaction.requiresReference() &&
            (await this.referenceAlreadyReversed(
              transaction.kind,
              reference.id,
              trx,
            ))
          ) {
            transaction.reject('REFERENCE_ALREADY_REVERSED');
            await this.persistWagerStatus(
              transaction,
              wallet.balance,
              reference.id,
              trx,
            );
            await this.insertRejectedEvent(
              transaction,
              wallet.id,
              createdAt,
              trx,
              correlationId,
            );
            return complete({
              httpStatus: 422,
              body: {
                transactionId: transaction.id,
                status: transaction.status,
                balance: wallet.balance.toJSON(),
                idempotentReplay: false,
              },
            });
          }
        }

        let change:
          | ReturnType<Wallet['credit']>
          | ReturnType<Wallet['debit']>
          | undefined;

        if (transaction.affectsBalance()) {
          const direction = transaction.ledgerDirectionFor(reference);
          try {
            change =
              direction === 'CREDIT'
                ? wallet.credit(money, createdAt)
                : wallet.debit(money, createdAt);
          } catch (error) {
            if (!(error instanceof InsufficientWalletBalanceError)) {
              throw error;
            }
            const failureCode: FailureCode = transaction.kind === WagerTransactionKind.Rollback
              ? 'REVERSAL_WOULD_OVERDRAW'
              : 'INSUFFICIENT_FUNDS';
            transaction.reject(failureCode);
          }
        }

        if (transaction.status !== WagerTransactionStatus.Rejected) {
          transaction.markProcessed(reference?.id, createdAt);
        }

        if (change !== undefined && change !== null) {
          const ledger = WalletLedgerEntry.create({
            id: randomUUID(),
            walletId: wallet.id,
            transactionId: transaction.id,
            direction:
              change.direction === 'CREDIT'
                ? LedgerDirection.Credit
                : LedgerDirection.Debit,
            money: change.money,
            balanceBefore: change.balanceBefore,
            balanceAfter: change.balanceAfter,
            createdAt,
          });
          await this.query(
            `update "wallets"
             set "balance" = ?, "version" = ?, "updated_at" = ?
             where "id" = ?`,
            [
              wallet.balance.toJSON().amount,
              wallet.version,
              wallet.updatedAt,
               wallet.id,
            ],
            trx,
          );
          await this.insertLedger(ledger, trx);
          await this.insertOutbox(
            'WalletBalanceChanged',
            wallet.id,
            {
              transactionId: transaction.id,
              walletId: wallet.id,
              direction: ledger.direction,
              money: ledger.money.toJSON(),
              balanceBefore: ledger.balanceBefore.toJSON(),
              balanceAfter: ledger.balanceAfter.toJSON(),
              walletVersion: wallet.version,
            },
            createdAt,
            trx,
            correlationId,
          );
        }

        await this.persistWagerStatus(
          transaction,
          wallet.balance,
          reference?.id,
          trx,
        );
        if (transaction.status === WagerTransactionStatus.Rejected) {
          await this.insertRejectedEvent(
            transaction,
            wallet.id,
            createdAt,
            trx,
            correlationId,
          );
        } else {
          await this.insertOutbox(
            'WagerTransactionProcessed',
            wallet.id,
            {
              transactionId: transaction.id,
              walletId: wallet.id,
              kind: transaction.kind,
              status: transaction.status,
              balance: wallet.balance.toJSON(),
            },
            createdAt,
            trx,
            correlationId,
          );
        }

        return complete({
          httpStatus: this.httpStatusFor(transaction.status),
          body: {
            transactionId: transaction.id,
            status: transaction.status,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
          },
        });
      });
    } catch (error) {
      this.rethrowDatabaseError(error);
    } finally {
      this.metrics.observeProcessingDuration(
        (performance.now() - startedAt) / 1000,
      );
    }
  }

  async getWallet(walletId: string): Promise<DbRow> {
    const rows = await this.query(
      `select "id", "player_id", "currency", "balance", "version",
              "created_at", "updated_at"
       from "wallets" where "id" = ?`,
      [walletId],
    );
    const wallet = rows[0];
    if (wallet === undefined) {
      throw new NotFoundException('Wallet not found');
    }
    return {
      id: wallet.id,
      playerId: wallet.player_id,
      balance: {
        amount: Money.from({
          amount: this.databaseDecimal(wallet.balance),
          currency: String(wallet.currency),
        }).toJSON().amount,
        currency: wallet.currency,
      },
      version: wallet.version,
      createdAt: wallet.created_at,
      updatedAt: wallet.updated_at,
    };
  }

  async getTransaction(transactionId: string): Promise<DbRow> {
    const rows = await this.query(
      `select * from "wager_transactions" where "id" = ?`,
      [transactionId],
    );
    if (rows[0] === undefined) {
      throw new NotFoundException('Wager transaction not found');
    }
    return this.transactionResponse(rows[0]);
  }

  async getProviderTransaction(
    providerId: string,
    externalTransactionId: string,
  ): Promise<DbRow> {
    const rows = await this.query(
      `select * from "wager_transactions"
       where "provider_id" = ? and "external_transaction_id" = ?`,
      [providerId, externalTransactionId],
    );
    if (rows[0] === undefined) {
      throw new NotFoundException('Wager transaction not found');
    }
    return this.transactionResponse(rows[0]);
  }

  async reconcileWallet(walletId: string): Promise<DbRow> {
    const wallets = await this.query(
      `select "currency", "balance" from "wallets" where "id" = ?`,
      [walletId],
    );
    const wallet = wallets[0];
    if (wallet === undefined) {
      throw new NotFoundException('Wallet not found');
    }

    const ledgerRows = await this.query(
      `select count(*)::text as entry_count,
              coalesce(sum(case when "direction" = 'CREDIT'
                                then "amount" else -"amount" end), 0)::text
                as balance
       from "wallet_ledger_entries" where "wallet_id" = ?`,
      [walletId],
    );
    const ledger = ledgerRows[0];
    if (ledger === undefined) {
      throw new Error('Ledger reconciliation query returned no result');
    }

    const currency = String(wallet.currency);
    const storedBalance = Money.from({
      amount: this.databaseDecimal(wallet.balance),
      currency,
    });
    const ledgerBalance = this.signedMoney(
      this.databaseDecimal(ledger.balance),
      currency,
    );
    const difference = storedBalance.subtract(ledgerBalance);
    const consistent = storedBalance.equals(ledgerBalance);
    if (!consistent) {
      this.logger.error(
        JSON.stringify({
          event: 'wallet_reconciliation_mismatch',
          walletId,
        }),
      );
    }

    return {
      walletId,
      consistent,
      storedBalance: storedBalance.toJSON(),
      ledgerBalance: ledgerBalance.toJSON(),
      difference: difference.toJSON(),
      ledgerEntries: Number(ledger.entry_count),
    };
  }

  async getLedger(
    walletId: string,
    cursor: string | undefined,
    requestedLimit: string | undefined,
  ): Promise<DbRow> {
    const parsedLimit = requestedLimit === undefined ? 50 : Number(requestedLimit);
    if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
      throw new UnprocessableEntityException('limit must be an integer from 1 to 100');
    }
    const wallet = await this.query(
      `select "id" from "wallets" where "id" = ?`,
      [walletId],
    );
    if (wallet.length === 0) {
      throw new NotFoundException('Wallet not found');
    }

    let cursorValues: [Date, string] | undefined;
    if (cursor !== undefined) {
      try {
        const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as unknown;
        if (
          !Array.isArray(decoded) ||
          decoded.length !== 2 ||
          typeof decoded[0] !== 'string' ||
          typeof decoded[1] !== 'string' ||
          Number.isNaN(Date.parse(decoded[0])) ||
          !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            decoded[1],
          )
        ) {
          throw new Error('invalid cursor');
        }
        cursorValues = [new Date(decoded[0]), decoded[1]];
      } catch {
        throw new UnprocessableEntityException('Invalid ledger cursor');
      }
    }

    const rows =
      cursorValues === undefined
        ? await this.query(
            `select * from "wallet_ledger_entries"
             where "wallet_id" = ?
             order by "created_at" desc, "id" desc limit ?`,
            [walletId, parsedLimit + 1],
          )
        : await this.query(
            `select * from "wallet_ledger_entries"
             where "wallet_id" = ? and ("created_at", "id") < (?, ?)
             order by "created_at" desc, "id" desc limit ?`,
            [walletId, cursorValues[0], cursorValues[1], parsedLimit + 1],
          );

    const hasMore = rows.length > parsedLimit;
    const page = hasMore ? rows.slice(0, parsedLimit) : rows;
    const last = page[page.length - 1];
    return {
      items: page.map((row) => ({
        id: row.id,
        walletId: row.wallet_id,
        transactionId: row.transaction_id,
        direction: row.direction,
        money: {
          amount: this.databaseDecimal(row.amount),
          currency: row.currency,
        },
        balanceBefore: {
          amount: this.databaseDecimal(row.balance_before),
          currency: row.currency,
        },
        balanceAfter: {
          amount: this.databaseDecimal(row.balance_after),
          currency: row.currency,
        },
        createdAt: row.created_at,
      })),
      nextCursor:
        hasMore && last !== undefined
          ? Buffer.from(
              JSON.stringify([
                this.databaseDate(last.created_at).toISOString(),
                String(last.id),
              ]),
            ).toString('base64url')
          : null,
    };
  }

  async processPendingReferences(): Promise<void> {
    await this.rejectExpiredPendingReferences();

    const candidates = await this.transaction(async (trx) => {
      const rows = await this.query(
        `select * from "wager_transactions"
         where "status" = 'PENDING_REFERENCE'
           and "reference_expires_at" > now()
           and "reference_next_attempt_at" <= now()
         order by "reference_next_attempt_at", "created_at"
         limit 20
         for update skip locked`,
        [],
        trx,
      );
      for (const row of rows) {
        this.metrics.recordRetry('pending_reference');
        await this.query(
          `update "wager_transactions"
           set "reference_next_attempt_at" = now() + interval '60 seconds'
           where "id" = ?`,
          [row.id],
          trx,
        );
      }
      return rows;
    });

    for (const row of candidates) {
      const pending = this.rehydrateTransaction(row);
      if (pending.kind === WagerTransactionKind.Opening) {
        throw new Error('Opening transactions cannot wait for a reference');
      }
      await this.submitWager(
        {
          providerId: pending.providerId,
          externalTransactionId: pending.externalTransactionId,
          playerId: pending.playerId,
          walletId: pending.walletId,
          roundId: pending.roundId,
          gameId: pending.gameId,
          kind: pending.kind,
          money: pending.money.toJSON(),
          ...(pending.referenceExternalTransactionId === undefined
            ? {}
            : {
                referenceExternalTransactionId:
                  pending.referenceExternalTransactionId,
              }),
        },
        pending.idempotencyKey,
        undefined,
        true,
      );
    }
  }

  private async rejectExpiredPendingReferences(): Promise<void> {
    await this.transaction(async (trx) => {
      const expired = await this.query(
        `select * from "wager_transactions"
         where "status" = 'PENDING_REFERENCE'
           and "reference_expires_at" <= now()
         order by "wallet_id", "reference_expires_at", "id"
         limit 20
         for update skip locked`,
        [],
        trx,
      );

      for (const row of expired) {
        const referenceRows = await this.query(
          `select "status" from "wager_transactions"
           where "provider_id" = ? and "external_transaction_id" = ?`,
          [
            row.provider_id,
            row.reference_external_transaction_id,
          ],
          trx,
        );
        if (
          referenceRows[0]?.status === WagerTransactionStatus.Processed
        ) {
          continue;
        }

        const walletRows = await this.query(
          `select "balance", "currency" from "wallets"
           where "id" = ? for update`,
          [row.wallet_id],
          trx,
        );
        const walletRow = walletRows[0];
        if (walletRow === undefined) {
          throw new Error('Pending wager references a missing wallet');
        }

        const transaction = this.rehydrateTransaction(row);
        transaction.reject(
          referenceRows.length === 0
            ? 'REFERENCE_NOT_FOUND'
            : 'REFERENCE_NOT_PROCESSED',
        );
        this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
        const balance = Money.from({
          amount: this.databaseDecimal(walletRow.balance),
          currency: String(walletRow.currency),
        });
        await this.persistWagerStatus(
          transaction,
          balance,
          transaction.referenceTransactionId,
          trx,
        );
        await this.insertRejectedEvent(
          transaction,
          transaction.walletId,
          new Date(),
          trx,
        );
      }
    });
  }

  private validateSubmitCommand(command: SubmitWagerCommand): void {
    const fields: Array<[string, string, number]> = [
      ['providerId', command.providerId, 200],
      ['externalTransactionId', command.externalTransactionId, 300],
      ['playerId', command.playerId, 200],
      ['roundId', command.roundId, 300],
      ['gameId', command.gameId, 300],
    ];
    for (const [name, value, maxLength] of fields) {
      this.assertLength(value, name, maxLength);
    }
    if (
      typeof command.walletId !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        command.walletId,
      )
    ) {
      throw new UnprocessableEntityException('walletId must be a UUID');
    }

    if (
      command.kind === WagerTransactionKind.Refund ||
      command.kind === WagerTransactionKind.Rollback
    ) {
      if (command.referenceExternalTransactionId === undefined) {
        throw new UnprocessableEntityException(
          'This transaction kind requires a reference',
        );
      }
    }
    if (command.referenceExternalTransactionId !== undefined) {
      this.assertLength(
        command.referenceExternalTransactionId,
        'referenceExternalTransactionId',
        300,
      );
    }

    const kind = command.kind as string;
    if (
      !Object.values(WagerTransactionKind).includes(
        kind as WagerTransactionKind,
      ) ||
      kind === WagerTransactionKind.Opening
    ) {
      throw new UnprocessableEntityException('Unsupported transaction kind');
    }
  }

  private parseMoney(props: MoneyProps): Money {
    try {
      const money = Money.from(props);
      if (!/^\d{1,18}\.\d{2}$/.test(money.toJSON().amount)) {
        throw new UnprocessableEntityException(
          'Amount exceeds the supported PostgreSQL precision',
        );
      }
      return money;
    } catch (error) {
      if (error instanceof UnprocessableEntityException) {
        throw error;
      }
      if (error instanceof TypeError) {
        throw new UnprocessableEntityException(error.message);
      }
      throw error;
    }
  }

  private assertNonEmpty(value: string, field: string): void {
    if (typeof value !== 'string' || value.trim().length === 0) {
      throw new UnprocessableEntityException(`${field} must not be empty`);
    }
  }

  private assertLength(
    value: string,
    field: string,
    maxLength: number,
  ): void {
    this.assertNonEmpty(value, field);
    if (value.length > maxLength) {
      throw new UnprocessableEntityException(
        `${field} must not exceed ${maxLength} characters`,
      );
    }
  }

  private hashPayload(payload: Record<string, unknown>): string {
    const canonicalize = (value: unknown): unknown => {
      if (Array.isArray(value)) {
        return value.map(canonicalize);
      }
      if (value !== null && typeof value === 'object') {
        return Object.fromEntries(
          Object.entries(value)
            .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
            .map(([key, child]) => [key, canonicalize(child)]),
        );
      }
      return value;
    };
    return createHash('sha256')
      .update(JSON.stringify(canonicalize(payload)))
      .digest('hex');
  }

  private referenceMatches(
    transaction: WagerTransaction,
    reference: WagerTransaction,
  ): boolean {
    return (
      transaction.providerId === reference.providerId &&
      transaction.playerId === reference.playerId &&
      transaction.walletId === reference.walletId &&
      transaction.money.currency === reference.money.currency &&
      transaction.roundId === reference.roundId
    );
  }

  private referenceKindAllowed(
    transaction: WagerTransaction,
    reference: WagerTransaction,
  ): boolean {
    switch (transaction.kind) {
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return reference.kind === WagerTransactionKind.Bet;
      case WagerTransactionKind.Rollback:
        return (
          reference.kind === WagerTransactionKind.Bet ||
          reference.kind === WagerTransactionKind.Win ||
          reference.kind === WagerTransactionKind.Refund
        );
      default:
        return false;
    }
  }

  private async referenceAlreadyReversed(
    kind: WagerTransactionKind,
    referenceId: string,
    trx: Transaction,
  ): Promise<boolean> {
    const rows = await this.query(
      `select "id" from "wager_transactions"
       where "kind" = ? and "reference_transaction_id" = ?
         and "status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED')
       limit 1`,
      [kind, referenceId],
      trx,
    );
    return rows.length > 0;
  }

  private rehydrateTransaction(row: DbRow): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: String(row.id),
      providerId: String(row.provider_id),
      externalTransactionId: String(row.external_transaction_id),
      idempotencyKey: String(row.idempotency_key),
      payloadHash: String(row.payload_hash).trim(),
      walletId: String(row.wallet_id),
      playerId: String(row.player_id),
      roundId: String(row.round_id),
      gameId: String(row.game_id),
      kind: row.kind as WagerTransactionKind,
      money: Money.from({
        amount: this.databaseDecimal(row.amount),
        currency: String(row.currency),
      }),
      createdAt: this.databaseDate(row.created_at),
      status: row.status as WagerTransactionStatus,
      ...(row.reference_external_transaction_id === null
        ? {}
        : {
            referenceExternalTransactionId: String(
              row.reference_external_transaction_id,
            ),
          }),
      ...(row.reference_transaction_id === null
        ? {}
        : { referenceTransactionId: String(row.reference_transaction_id) }),
      ...(row.failure_code === null
        ? {}
        : { failureCode: row.failure_code as FailureCode }),
      ...(row.processed_at === null
        ? {}
        : { processedAt: this.databaseDate(row.processed_at) }),
    });
  }

  private async persistWagerStatus(
    transaction: WagerTransaction,
    balance: Money,
    referenceTransactionId: string | undefined,
    trx: Transaction,
  ): Promise<void> {
    await this.query(
      `update "wager_transactions"
       set "status" = ?, "reference_transaction_id" = ?,
           "failure_code" = ?, "processed_at" = ?,
           "result_balance" = ?, "result_currency" = ?,
           "reference_next_attempt_at" = null
       where "id" = ?`,
      [
        transaction.status,
        referenceTransactionId ?? null,
        transaction.failureCode ?? null,
        transaction.processedAt ?? null,
        balance.toJSON().amount,
        balance.currency,
         transaction.id,
      ],
      trx,
    );
  }

  private async schedulePendingReference(
    transactionId: string,
    trx: Transaction,
  ): Promise<void> {
    await this.query(
      `update "wager_transactions"
       set "reference_attempts" = "reference_attempts" + 1,
           "reference_next_attempt_at" = now()
             + make_interval(secs => least(power(2, least("reference_attempts", 6)), 60)),
           "reference_expires_at" = coalesce(
             "reference_expires_at",
             now() + interval '24 hours'
           )
       where "id" = ?`,
      [transactionId],
      trx,
    );
  }

  private async insertLedger(
    entry: WalletLedgerEntry,
    trx: Transaction,
  ): Promise<void> {
    await this.query(
      `insert into "wallet_ledger_entries"
        ("id", "wallet_id", "transaction_id", "direction", "amount",
         "currency", "balance_before", "balance_after", "created_at")
       values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.toJSON().amount,
        entry.money.currency,
        entry.balanceBefore.toJSON().amount,
        entry.balanceAfter.toJSON().amount,
        entry.createdAt,
      ],
      trx,
    );
  }

  private async insertRejectedEvent(
    transaction: WagerTransaction,
    walletId: string,
    at: Date,
    trx: Transaction,
    correlationId?: string,
  ): Promise<void> {
    await this.insertOutbox(
      'WagerTransactionRejected',
      walletId,
      {
        transactionId: transaction.id,
        walletId,
        providerId: transaction.providerId,
        kind: transaction.kind,
        failureCode: transaction.failureCode,
      },
      at,
      trx,
      correlationId,
    );
  }

  private async insertOutbox(
    eventType: IntegrationEventName,
    aggregateId: string,
    data: Record<string, unknown>,
    occurredAt: Date,
    trx: Transaction,
    correlationId?: string,
  ): Promise<void> {
    const eventId = randomUUID();
    const event = createIntegrationEvent(eventType, {
      eventId,
      aggregateId,
      correlationId: correlationId ?? eventId,
      data,
      occurredAt,
    });
    await this.query(
      `insert into "outbox_messages"
        ("id", "aggregate_id", "event_type", "payload", "occurred_at")
       values (?, ?, ?, ?::jsonb, ?)`,
      [
        eventId,
        aggregateId,
        event.eventType,
        JSON.stringify(event.toJSON()),
        occurredAt,
      ],
      trx,
    );
  }

  private transactionResponse(row: DbRow): DbRow {
    return {
      id: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      playerId: row.player_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      status: row.status,
      money: {
        amount: this.databaseDecimal(row.amount),
        currency: row.currency,
      },
      referenceExternalTransactionId: row.reference_external_transaction_id,
      referenceTransactionId: row.reference_transaction_id,
      failureCode: row.failure_code,
      createdAt: row.created_at,
      processedAt: row.processed_at,
      resultBalance:
        row.result_balance === null
          ? null
          : {
              amount: this.databaseDecimal(row.result_balance),
              currency: row.result_currency,
            },
    };
  }

  private httpStatusFor(status: WagerTransactionStatus): 200 | 202 | 422 {
    if (status === WagerTransactionStatus.PendingReference) {
      return 202;
    }
    if (status === WagerTransactionStatus.Rejected) {
      return 422;
    }
    return 200;
  }

  private persistedWagerResult(row: DbRow): WagerHttpResult {
    if (
      typeof row.result_balance !== 'string' ||
      typeof row.result_currency !== 'string'
    ) {
      throw new Error('Persisted idempotent response is incomplete');
    }

    const status = row.status as WagerTransactionStatus;
    return {
      httpStatus: this.httpStatusFor(status),
      body: {
        transactionId: String(row.id),
        status,
        balance: Money.from({
          amount: row.result_balance,
          currency: row.result_currency,
        }).toJSON(),
        idempotentReplay: true,
      },
    };
  }

  private databaseDecimal(value: unknown): string {
    if (typeof value !== 'string') {
      throw new TypeError('Expected PostgreSQL numeric value as a decimal string');
    }
    return value;
  }

  private signedMoney(amount: string, currency: string): Money {
    if (amount.startsWith('-')) {
      return Money.from({
        amount: amount.slice(1),
        currency,
      }).negate();
    }
    return Money.from({ amount, currency });
  }

  private databaseDate(value: unknown): Date {
    const date = value instanceof Date ? new Date(value.getTime()) : new Date(String(value));
    if (Number.isNaN(date.getTime())) {
      throw new TypeError('Database returned an invalid timestamp');
    }
    return date;
  }

  private async transaction<T>(callback: (trx: Transaction) => Promise<T>): Promise<T> {
    return this.entityManager.getConnection().transactional(callback);
  }

  private async query(
    sql: string,
    params: unknown[] = [],
    trx?: Transaction,
  ): Promise<DbRow[]> {
    const result = await this.entityManager
      .getConnection()
      .execute(sql, params, 'all', trx);
    if (!Array.isArray(result)) {
      throw new Error('Database query did not return rows');
    }
    return result as DbRow[];
  }

  private rethrowDatabaseError(error: unknown): never {
    const code = this.databaseErrorCode(error);
    if (code === '40001' || code === '40P01' || code === '55P03') {
      this.metrics.recordLockConflict();
    }
    if (code === '23505') {
      throw new ConflictException('A transaction with these identifiers already exists');
    }
    if (
      code?.startsWith('08') ||
      code === '57P01' ||
      code === '53300' ||
      code === '40001' ||
      code === '40P01' ||
      code === '55P03'
    ) {
      throw new ServiceUnavailableException('Database temporarily unavailable');
    }
    throw error;
  }

  private databaseErrorCode(error: unknown): string | undefined {
    if (error === null || typeof error !== 'object') {
      return undefined;
    }
    if ('code' in error && typeof error.code === 'string') {
      return error.code;
    }
    if ('cause' in error) {
      return this.databaseErrorCode(error.cause);
    }
    return undefined;
  }
}
