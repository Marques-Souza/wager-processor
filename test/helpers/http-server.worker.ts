import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';

async function main(): Promise<void> {
  const app = await NestFactory.create(AppModule, { logger: false });
  app.enableShutdownHooks();
  await app.listen(0, '127.0.0.1');

  const address = app.getHttpServer().address();
  if (address === null || typeof address === 'string') {
    throw new Error('HTTP server did not bind to a TCP port');
  }
  process.stdout.write(`${JSON.stringify({ port: address.port })}\n`);
}

void main();
