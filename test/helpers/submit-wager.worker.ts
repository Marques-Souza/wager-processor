import { HttpException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/core';
import mikroOrmConfig from '../../src/infrastructure/persistence/mikro-orm.config';
import {
  SubmitWagerCommand,
  WageringService,
} from '../../src/application/wagering/wagering.service';

interface WorkerInput {
  command: SubmitWagerCommand;
  idempotencyKey: string;
}

async function main(): Promise<void> {
  const input = JSON.parse(process.argv[2] ?? '') as WorkerInput;
  const orm = await MikroORM.init(mikroOrmConfig);
  try {
    const service = new WageringService(orm.em);
    for (let attempt = 0; ; attempt += 1) {
      try {
        const result = await service.submitWager(
          input.command,
          input.idempotencyKey,
        );
        process.stdout.write(JSON.stringify(result));
        return;
      } catch (error) {
        if (
          !(error instanceof HttpException) ||
          error.getStatus() < 500 ||
          attempt >= 10
        ) {
          throw error;
        }
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(1000, 50 * 2 ** attempt)),
        );
      }
    }
  } finally {
    await orm.close(true);
  }
}

void main();
