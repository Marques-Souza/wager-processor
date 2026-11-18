import { randomUUID } from 'node:crypto';
import { Injectable, type NestMiddleware } from '@nestjs/common';
import type { NextFunction, Request, Response } from 'express';

export interface CorrelatedRequest extends Request {
  correlationId: string;
}

@Injectable()
export class CorrelationIdMiddleware implements NestMiddleware {
  use(request: Request, response: Response, next: NextFunction): void {
    const suppliedId = request.header('x-correlation-id');
    const correlationId =
      suppliedId !== undefined && /^[A-Za-z0-9._:-]{1,128}$/.test(suppliedId)
        ? suppliedId
        : randomUUID();

    (request as CorrelatedRequest).correlationId = correlationId;
    response.setHeader('x-correlation-id', correlationId);
    next();
  }
}
