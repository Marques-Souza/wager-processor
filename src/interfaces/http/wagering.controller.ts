import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Logger,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  Req,
  Res,
  UnprocessableEntityException,
  UseGuards,
} from '@nestjs/common';
import type { Response } from 'express';
import {
  CreateWalletCommand,
  SubmitWagerCommand,
  WageringService,
} from '../../application/wagering/wagering.service';
import { WagerTransactionKind } from '../../domain/entities/wager-transaction';
import { MoneyProps } from '../../domain/value-objects/money';
import { AuthGuard } from './auth.guard';
import type { CorrelatedRequest } from './correlation-id.middleware';

@Controller()
@UseGuards(AuthGuard)
export class WageringController {
  private readonly logger = new Logger(WageringController.name);

  constructor(private readonly wageringService: WageringService) {}

  @Post('wallets')
  @HttpCode(201)
  createWallet(@Body() body: unknown) {
    return this.wageringService.createWallet(this.parseCreateWallet(body));
  }

  @Get('wallets/:walletId')
  getWallet(@Param('walletId', ParseUUIDPipe) walletId: string) {
    return this.wageringService.getWallet(walletId);
  }

  @Get('wallets/:walletId/ledger')
  getLedger(
    @Param('walletId', ParseUUIDPipe) walletId: string,
    @Query('cursor') cursor: string | undefined,
    @Query('limit') limit: string | undefined,
  ) {
    return this.wageringService.getLedger(walletId, cursor, limit);
  }

  @Post('wallets/:walletId/reconciliation')
  async reconcileWallet(
    @Param('walletId', ParseUUIDPipe) walletId: string,
    @Req() request: CorrelatedRequest,
  ) {
    const result = await this.wageringService.reconcileWallet(walletId);
    this.logger.log(
      JSON.stringify({
        event: 'wallet_reconciliation_checked',
        correlationId: request.correlationId,
        walletId,
        consistent: result.consistent,
      }),
    );
    return result;
  }

  @Post('wagering/transactions')
  @HttpCode(200)
  async submitWager(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) response: Response,
    @Req() request: CorrelatedRequest,
  ) {
    if (idempotencyKey === undefined || idempotencyKey.trim().length === 0) {
      throw new UnprocessableEntityException('Idempotency-Key header is required');
    }

    const command = this.parseSubmitWager(body);
    const result = await this.wageringService.submitWager(
      command,
      idempotencyKey,
      undefined,
      false,
      request.correlationId,
    );
    this.logger.log(
      JSON.stringify({
        event: 'wager_transaction_submitted',
        correlationId: request.correlationId,
        transactionId: result.body.transactionId,
        walletId: command.walletId,
        providerId: command.providerId,
        status: result.body.status,
        idempotentReplay: result.body.idempotentReplay,
      }),
    );
    response.status(result.httpStatus);
    return result.body;
  }

  @Get('wagering/transactions/:transactionId')
  getTransaction(
    @Param('transactionId', ParseUUIDPipe) transactionId: string,
  ) {
    return this.wageringService.getTransaction(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  getProviderTransaction(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    return this.wageringService.getProviderTransaction(
      providerId,
      externalTransactionId,
    );
  }

  private parseCreateWallet(body: unknown): CreateWalletCommand {
    const record = this.asRecord(body);
    return {
      playerId: this.stringField(record, 'playerId'),
      initialBalance: this.moneyField(record.initialBalance),
    };
  }

  private parseSubmitWager(body: unknown): SubmitWagerCommand {
    const record = this.asRecord(body);
    const kind = this.stringField(record, 'kind');
    const validKinds = [
      WagerTransactionKind.Bet,
      WagerTransactionKind.Win,
      WagerTransactionKind.Loss,
      WagerTransactionKind.Refund,
      WagerTransactionKind.Rollback,
    ];
    if (!validKinds.includes(kind as (typeof validKinds)[number])) {
      throw new UnprocessableEntityException('Unsupported transaction kind');
    }

    const reference = record.referenceExternalTransactionId;
    if (reference !== undefined && typeof reference !== 'string') {
      throw new UnprocessableEntityException(
        'referenceExternalTransactionId must be a string',
      );
    }

    return {
      providerId: this.stringField(record, 'providerId'),
      externalTransactionId: this.stringField(record, 'externalTransactionId'),
      playerId: this.stringField(record, 'playerId'),
      walletId: this.stringField(record, 'walletId'),
      roundId: this.stringField(record, 'roundId'),
      gameId: this.stringField(record, 'gameId'),
      kind: kind as SubmitWagerCommand['kind'],
      money: this.moneyField(record.money),
      ...(reference === undefined
        ? {}
        : { referenceExternalTransactionId: reference }),
    };
  }

  private asRecord(value: unknown): Record<string, unknown> {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new UnprocessableEntityException('Request body must be an object');
    }
    return value as Record<string, unknown>;
  }

  private stringField(
    record: Record<string, unknown>,
    field: string,
  ): string {
    const value = record[field];
    if (typeof value !== 'string' || value.trim().length === 0) {
      throw new UnprocessableEntityException(`${field} must be a non-empty string`);
    }
    return value;
  }

  private moneyField(value: unknown): MoneyProps {
    const record = this.asRecord(value);
    return {
      amount: this.stringField(record, 'amount'),
      currency: this.stringField(record, 'currency'),
    };
  }
}
