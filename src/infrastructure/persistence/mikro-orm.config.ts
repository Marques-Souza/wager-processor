import { Migrator } from '@mikro-orm/migrations';
import { defineConfig } from '@mikro-orm/postgresql';

export default defineConfig({
  host: process.env.DB_HOST ?? 'localhost',
  port: Number.parseInt(process.env.DB_PORT ?? '5433', 10),
  dbName: process.env.DB_NAME ?? 'wager_processor',
  user: process.env.DB_USER ?? 'wager',
  password: process.env.DB_PASSWORD ?? 'wager_dev_only',
  entities: ['./dist/**/*.entity.js'],
  entitiesTs: ['./src/**/*.entity.ts'],
  extensions: [Migrator],
  discovery: {
    warnWhenNoEntities: false,
  },
  migrations: {
    path: './dist/infrastructure/persistence/migrations',
    pathTs: './src/infrastructure/persistence/migrations',
  },
});
