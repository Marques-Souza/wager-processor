import {
  Controller,
  Get,
  Logger,
  OnModuleDestroy,
  ServiceUnavailableException,
} from '@nestjs/common';
import { EntityManager } from '@mikro-orm/core';
import { SQSClient, ListQueuesCommand } from '@aws-sdk/client-sqs';
import { Inject } from '@nestjs/common';

@Controller('health')
export class HealthController implements OnModuleDestroy {
  private readonly logger = new Logger(HealthController.name);
  private readonly sqsClient = new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });

  constructor(
    @Inject(EntityManager)
    private readonly entityManager: EntityManager,
  ) {}

  onModuleDestroy(): void {
    this.sqsClient.destroy();
  }

  @Get('live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(): Promise<{ status: 'ready' }> {
    try {
      await this.entityManager.getConnection().execute('select 1');
      await this.sqsClient.send(new ListQueuesCommand({}));
      return { status: 'ready' };
    } catch (error) {
      this.logger.error(
        'Readiness check failed',
        error instanceof Error ? error.stack : String(error),
      );
      throw new ServiceUnavailableException({
        status: 'not_ready',
        reason: 'A required dependency is unavailable',
      });
    }
  }
}
