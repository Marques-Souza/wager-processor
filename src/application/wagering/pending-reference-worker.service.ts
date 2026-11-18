import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { WageringService } from './wagering.service';

@Injectable()
export class PendingReferenceWorkerService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(PendingReferenceWorkerService.name);
  private stopped = false;
  private loopPromise: Promise<void> | undefined;

  constructor(private readonly wageringService: WageringService) {}

  onModuleInit(): void {
    this.loopPromise = this.run();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    await this.loopPromise;
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      try {
        await this.wageringService.processPendingReferences();
      } catch (error) {
        this.logger.error(
          JSON.stringify({
            event: 'pending_reference_worker_failed',
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }

      if (!this.stopped) {
        await new Promise<void>((resolve) => setTimeout(resolve, 5000));
      }
    }
  }
}
