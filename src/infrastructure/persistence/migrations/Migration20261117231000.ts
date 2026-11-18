import { Migration } from '@mikro-orm/migrations';

export class Migration20261117231000 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table "wallets" (
        "id" uuid primary key,
        "player_id" varchar(200) not null,
        "currency" char(3) not null check ("currency" ~ '^[A-Z]{3}$'),
        "balance" numeric(20, 2) not null default 0 check ("balance" >= 0),
        "version" integer not null check ("version" >= 1),
        "created_at" timestamptz not null,
        "updated_at" timestamptz not null,
        constraint "wallets_player_currency_unique" unique ("player_id", "currency"),
        constraint "wallets_id_currency_unique" unique ("id", "currency"),
        constraint "wallets_id_player_currency_unique" unique ("id", "player_id", "currency")
      );
    `);

    this.addSql(`
      create table "wager_transactions" (
        "id" uuid primary key,
        "provider_id" varchar(200) not null,
        "external_transaction_id" varchar(300) not null,
        "idempotency_key" varchar(300) not null,
        "payload_hash" char(64) not null,
        "wallet_id" uuid not null,
        "player_id" varchar(200) not null,
        "currency" char(3) not null check ("currency" ~ '^[A-Z]{3}$'),
        "round_id" varchar(300) not null,
        "game_id" varchar(300) not null,
        "kind" varchar(20) not null check ("kind" in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        "status" varchar(30) not null check ("status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        "amount" numeric(20, 2) not null check ("amount" > 0),
        "reference_external_transaction_id" varchar(300),
        "reference_transaction_id" uuid references "wager_transactions" ("id"),
        "reference_attempts" integer not null default 0 check ("reference_attempts" >= 0),
        "reference_next_attempt_at" timestamptz,
        "reference_expires_at" timestamptz,
        "failure_code" varchar(80),
        "created_at" timestamptz not null,
        "processed_at" timestamptz,
        "result_balance" numeric(20, 2),
        "result_currency" char(3),
        constraint "wager_transactions_idempotency_unique" unique ("idempotency_key"),
        constraint "wager_transactions_provider_external_unique" unique ("provider_id", "external_transaction_id"),
        constraint "wager_transactions_id_wallet_currency_unique"
          unique ("id", "wallet_id", "currency"),
        constraint "wager_transactions_wallet_identity_fk"
          foreign key ("wallet_id", "player_id", "currency")
          references "wallets" ("id", "player_id", "currency"),
        constraint "wager_transactions_reference_required" check (
          ("kind" in ('REFUND', 'ROLLBACK')
            and "reference_external_transaction_id" is not null
            and length(btrim("reference_external_transaction_id")) > 0)
          or ("kind" not in ('REFUND', 'ROLLBACK')
            and ("reference_external_transaction_id" is null
              or length(btrim("reference_external_transaction_id")) > 0))
        ),
        constraint "wager_transactions_result_pair_check" check (
          ("result_balance" is null and "result_currency" is null)
          or ("result_balance" is not null and "result_currency" is not null)
        ),
        constraint "wager_transactions_result_balance_check" check (
          "result_balance" is null or "result_balance" >= 0
        ),
        constraint "wager_transactions_result_currency_check" check (
          "result_currency" is null or "result_currency" = "currency"
        )
      );
    `);

    this.addSql(`
      create unique index "wager_transactions_refund_once"
        on "wager_transactions" ("reference_transaction_id")
        where "kind" = 'REFUND' and "status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED');
      create unique index "wager_transactions_rollback_once"
        on "wager_transactions" ("reference_transaction_id")
        where "kind" = 'ROLLBACK' and "status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED');
      create index "wager_transactions_pending_reference_idx"
        on "wager_transactions" ("reference_next_attempt_at", "created_at")
        where "status" = 'PENDING_REFERENCE';
    `);

    this.addSql(`
      create table "wallet_ledger_entries" (
        "id" uuid primary key,
        "wallet_id" uuid not null,
        "transaction_id" uuid not null,
        "direction" varchar(10) not null check ("direction" in ('DEBIT', 'CREDIT')),
        "amount" numeric(20, 2) not null check ("amount" > 0),
        "currency" char(3) not null check ("currency" ~ '^[A-Z]{3}$'),
        "balance_before" numeric(20, 2) not null check ("balance_before" >= 0),
        "balance_after" numeric(20, 2) not null check ("balance_after" >= 0),
        "created_at" timestamptz not null,
        constraint "wallet_ledger_wallet_currency_fk"
          foreign key ("wallet_id", "currency")
          references "wallets" ("id", "currency"),
        constraint "wallet_ledger_transaction_wallet_currency_fk"
          foreign key ("transaction_id", "wallet_id", "currency")
          references "wager_transactions" ("id", "wallet_id", "currency"),
        constraint "wallet_ledger_transaction_wallet_unique" unique ("transaction_id", "wallet_id"),
        constraint "wallet_ledger_balance_matches_direction" check (
          ("direction" = 'CREDIT' and "balance_after" = "balance_before" + "amount")
          or ("direction" = 'DEBIT' and "balance_after" = "balance_before" - "amount")
        )
      );
    `);

    this.addSql(`
      create index "wallet_ledger_wallet_cursor_idx"
        on "wallet_ledger_entries" ("wallet_id", "created_at" desc, "id" desc);

      create function "prevent_wallet_ledger_mutation"()
      returns trigger language plpgsql as $$
      begin
        raise exception 'wallet ledger entries are immutable';
      end;
      $$;

      create trigger "wallet_ledger_immutable"
        before update or delete on "wallet_ledger_entries"
        for each row execute function "prevent_wallet_ledger_mutation"();
    `);

    this.addSql(`
      create table "inbox_messages" (
        "consumer_name" varchar(200) not null,
        "message_id" varchar(300) not null,
        "payload_hash" char(64) not null,
        "received_at" timestamptz not null,
        "processed_at" timestamptz,
        primary key ("consumer_name", "message_id")
      );

      create table "outbox_messages" (
        "id" uuid primary key,
        "aggregate_id" uuid not null,
        "event_type" varchar(200) not null,
        "payload" jsonb not null,
        "occurred_at" timestamptz not null,
        "attempts" integer not null default 0 check ("attempts" >= 0),
        "next_attempt_at" timestamptz,
        "published_at" timestamptz
      );

      create index "outbox_pending_idx"
        on "outbox_messages" ("next_attempt_at", "occurred_at")
        where "published_at" is null;
    `);
  }

  override async down(): Promise<void> {
    this.addSql('drop table if exists "outbox_messages";');
    this.addSql('drop table if exists "inbox_messages";');
    this.addSql('drop trigger if exists "wallet_ledger_immutable" on "wallet_ledger_entries";');
    this.addSql('drop function if exists "prevent_wallet_ledger_mutation"();');
    this.addSql('drop table if exists "wallet_ledger_entries";');
    this.addSql('drop table if exists "wager_transactions";');
    this.addSql('drop table if exists "wallets";');
  }
}
