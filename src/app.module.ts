import { Module, type MiddlewareConsumer, type NestModule } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { WageringService } from './application/wagering/wagering.service';
import { HealthController } from './interfaces/http/health.controller';
import { WageringController } from './interfaces/http/wagering.controller';
import mikroOrmConfig from './infrastructure/persistence/mikro-orm.config';
import { OutboxPublisherService } from './infrastructure/messaging/outbox-publisher.service';
import { WagerTransactionConsumerService } from './infrastructure/messaging/wager-transaction-consumer.service';
import { PendingReferenceWorkerService } from './application/wagering/pending-reference-worker.service';
import { AuthGuard } from './interfaces/http/auth.guard';
import { MetricsService } from './application/observability/metrics.service';
import { MetricsController } from './interfaces/http/metrics.controller';
import { CorrelationIdMiddleware } from './interfaces/http/correlation-id.middleware';

@Module({
  imports: [MikroOrmModule.forRoot(mikroOrmConfig)],
  controllers: [WageringController, HealthController, MetricsController],
  providers: [
    WageringService,
    MetricsService,
    AuthGuard,
    PendingReferenceWorkerService,
    OutboxPublisherService,
    WagerTransactionConsumerService,
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer
      .apply(CorrelationIdMiddleware)
      .forRoutes(WageringController, HealthController, MetricsController);
  }
}
