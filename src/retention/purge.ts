/**
 * src/retention/purge.ts
 *
 * Scheduled Data Retention Purge Job — Issue #261
 *
 * Executes a sequential, small-batch deletion loop over three tables:
 *   1. event_processing_audit  (governed by RETENTION_EVENTS_DAYS)
 *   2. webhook_dlq             (governed by RETENTION_WEBHOOKS_DAYS)
 *   3. purge_audit_log         (governed by RETENTION_DLQ_DAYS)
 *
 * Each table is processed in isolated micro-transactions (LIMIT 100 per
 * batch) so the SQLite WAL file never grows large enough to stall the
 * production hot path.
 *
 * Execution metrics are sanitised through src/redact.ts before being
 * written to a tamper-evident audit record via ComplianceAuditLogger.
 */

import * as crypto from 'crypto';
import { getDb } from '../db/database';
import { ComplianceAuditLogger } from './audit';
import { DataEntityType, RetentionAction } from './types';
import { redactUrl } from '../redact';

// ─── Config ────────────────────────────────────────────────────────────────

/**
 * Per-table retention windows parsed from environment variables.
 * Defaults are intentionally conservative (90 / 30 / 7 days).
 */
export interface PurgeConfig {
  /** Days to keep event_processing_audit rows (default: 90) */
  eventsDays: number;
  /** Days to keep webhook_dlq rows (default: 30) */
  webhooksDays: number;
  /** Days to keep rows in the purge_audit_log itself (default: 7) */
  dlqDays: number;
  /** Rows removed per micro-transaction batch (default: 100) */
  batchSize: number;
}

export function loadPurgeConfig(): PurgeConfig {
  function parseDays(envKey: string, fallback: number): number {
    const raw = process.env[envKey];
    if (!raw) return fallback;
    const n = parseInt(raw, 10);
    if (!Number.isFinite(n) || n < 1) {
      throw new Error(`[purge] ${envKey} must be a positive integer, got "${raw}"`);
    }
    return n;
  }

  return {
    eventsDays:   parseDays('RETENTION_EVENTS_DAYS',   90),
    webhooksDays: parseDays('RETENTION_WEBHOOKS_DAYS',  30),
    dlqDays:      parseDays('RETENTION_DLQ_DAYS',        7),
    batchSize:    100,
  };
}

// ─── Types ──────────────────────────────────────────────────────────────────

/** Metrics produced by a single-table purge pass. */
export interface TablePurgeMetrics {
  table: string;
  cutoffIso: string;
  batchesRun: number;
  rowsDeleted: number;
  durationMs: number;
}

/** Aggregate result returned by runDataRetentionPurge(). */
export interface PurgeRunResult {
  runId: string;
  startedAt: string;
  completedAt: string;
  totalRowsDeleted: number;
  tables: TablePurgeMetrics[];
  auditLogId: string;
}

// ─── Table migration guard ───────────────────────────────────────────────────

/**
 * Ensures that both the webhook_dlq table and the purge_audit_log table
 * exist.  Called once per purge run so the job is self-bootstrapping.
 */
function ensureTables(): void {
  const db = getDb();

  // webhook_dlq — created by WebhookDLQStorage but we may run standalone
  db.exec(`
    CREATE TABLE IF NOT EXISTS webhook_dlq (
      id          TEXT PRIMARY KEY,
      webhook_id  TEXT NOT NULL,
      url         TEXT NOT NULL,
      body        TEXT NOT NULL,
      retry_count INTEGER NOT NULL DEFAULT 0,
      webhook_secret TEXT,
      failed_at   TEXT NOT NULL,
      last_error  TEXT NOT NULL,
      dedupe_key  TEXT NOT NULL,
      replayed_at TEXT,
      created_at  TEXT NOT NULL,
      updated_at  TEXT NOT NULL,
      UNIQUE(dedupe_key)
    );
    CREATE INDEX IF NOT EXISTS idx_webhook_dlq_failed_at
      ON webhook_dlq(failed_at);
  `);

  // purge_audit_log — durable record of every purge run
  db.exec(`
    CREATE TABLE IF NOT EXISTS purge_audit_log (
      id            TEXT PRIMARY KEY,
      run_id        TEXT NOT NULL,
      table_name    TEXT NOT NULL,
      cutoff_iso    TEXT NOT NULL,
      batches_run   INTEGER NOT NULL,
      rows_deleted  INTEGER NOT NULL,
      duration_ms   INTEGER NOT NULL,
      hmac_proof    TEXT NOT NULL,
      created_at    TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_purge_audit_log_created_at
      ON purge_audit_log(created_at);
  `);
}

// ─── Core helpers ────────────────────────────────────────────────────────────

/** ISO-8601 cutoff timestamp for a given retention window. */
function cutoffFor(days: number): string {
  return new Date(Date.now() - days * 86_400_000).toISOString();
}

/**
 * Purges expired rows from a single table in LIMIT-100 micro-batches.
 *
 * @param table      - Table name (safe: caller-controlled constant, never user input)
 * @param dateColumn - Column holding the row's timestamp
 * @param cutoffIso  - Delete rows older than this ISO-8601 string
 * @param batchSize  - Max rows per micro-transaction
 */
function purgeTable(
  table: string,
  dateColumn: string,
  cutoffIso: string,
  batchSize: number,
): Omit<TablePurgeMetrics, 'durationMs'> & { startMs: number } {
  const db = getDb();
  const startMs = Date.now();

  // Use a rowid-based subquery so we only hold a short-lived read lock
  // while building the candidate set, then delete in a separate write step.
  const selectBatch = db.prepare<[string, number]>(`
    SELECT rowid FROM ${table}
    WHERE ${dateColumn} < ?
    ORDER BY ${dateColumn} ASC
    LIMIT ?
  `);

  const deleteByRowid = db.prepare<[number]>(`
    DELETE FROM ${table} WHERE rowid = ?
  `);

  const runBatch = db.transaction((cutoff: string, limit: number): number => {
    const rows = selectBatch.all(cutoff, limit) as { rowid: number }[];
    for (const row of rows) {
      deleteByRowid.run(row.rowid);
    }
    return rows.length;
  });

  let batchesRun = 0;
  let rowsDeleted = 0;
  let lastBatchSize: number;

  do {
    lastBatchSize = runBatch(cutoffIso, batchSize) as number;
    rowsDeleted += lastBatchSize;
    if (lastBatchSize > 0) batchesRun++;
  } while (lastBatchSize === batchSize); // keep going until a partial batch

  return { table, cutoffIso, batchesRun, rowsDeleted, startMs };
}

// ─── Audit helpers ───────────────────────────────────────────────────────────

/** Sanitise metric values before they reach the audit log. */
function redactMetrics(metrics: TablePurgeMetrics[]): Record<string, unknown>[] {
  return metrics.map((m) => ({
    table:        m.table,          // never user-supplied; safe
    cutoffIso:    redactUrl(m.cutoffIso), // strip any accidental query params
    batchesRun:   m.batchesRun,
    rowsDeleted:  m.rowsDeleted,
    durationMs:   m.durationMs,
  }));
}

/** HMAC-SHA256 proof over a single table metrics row. */
function hmacProof(row: Record<string, unknown>): string {
  const secret =
    process.env['COMPLIANCE_AUDIT_SECRET'] ??
    'talenttrust-compliance-secret-key-2024';
  return crypto
    .createHmac('sha256', secret)
    .update(JSON.stringify(row))
    .digest('hex');
}

/** Persist one row per table into purge_audit_log. */
function writePurgeAuditLog(
  runId: string,
  redacted: Record<string, unknown>[],
): void {
  const db = getDb();
  const insert = db.prepare<[string, string, string, string, number, number, number, string, string]>(`
    INSERT INTO purge_audit_log
      (id, run_id, table_name, cutoff_iso, batches_run, rows_deleted, duration_ms, hmac_proof, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const now = new Date().toISOString();
  const writeAll = db.transaction(() => {
    for (const row of redacted) {
      insert.run(
        crypto.randomUUID(),
        runId,
        row['table'] as string,
        row['cutoffIso'] as string,
        row['batchesRun'] as number,
        row['rowsDeleted'] as number,
        row['durationMs'] as number,
        hmacProof(row),
        now,
      );
    }
  });
  writeAll();
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Run the full data-retention purge workflow.
 *
 * Tables are processed sequentially (events → webhooks → dlq) so a slow
 * batch in one table cannot block reads on another.  Each batch is its own
 * micro-transaction; no long-running write lock is held.
 *
 * @param config   - Optional override; defaults to environment-variable values.
 * @param logger   - Optional ComplianceAuditLogger instance (injectable for tests).
 * @returns        - Aggregated run metrics and the audit log entry ID.
 */
export async function runDataRetentionPurge(
  config: PurgeConfig = loadPurgeConfig(),
  logger: ComplianceAuditLogger = new ComplianceAuditLogger(),
): Promise<PurgeRunResult> {
  const runId = crypto.randomUUID();
  const startedAt = new Date().toISOString();

  ensureTables();

  // ── 1. events ────────────────────────────────────────────────────────────
  const eventsRaw = purgeTable(
    'event_processing_audit',
    'created_at',
    cutoffFor(config.eventsDays),
    config.batchSize,
  );

  // ── 2. webhook deliveries ─────────────────────────────────────────────────
  const webhooksRaw = purgeTable(
    'webhook_dlq',
    'created_at',
    cutoffFor(config.webhooksDays),
    config.batchSize,
  );

  // ── 3. purge audit log (self-pruning) ─────────────────────────────────────
  const dlqRaw = purgeTable(
    'purge_audit_log',
    'created_at',
    cutoffFor(config.dlqDays),
    config.batchSize,
  );

  const now = Date.now();
  const tableMetrics: TablePurgeMetrics[] = [
    { ...eventsRaw,   durationMs: now - eventsRaw.startMs   },
    { ...webhooksRaw, durationMs: now - webhooksRaw.startMs },
    { ...dlqRaw,      durationMs: now - dlqRaw.startMs      },
  ].map(({ startMs: _drop, ...rest }) => rest as TablePurgeMetrics);

  const totalRowsDeleted = tableMetrics.reduce((s, t) => s + t.rowsDeleted, 0);
  const completedAt = new Date().toISOString();

  // ── Sanitise and persist audit record ────────────────────────────────────
  const redacted = redactMetrics(tableMetrics);
  writePurgeAuditLog(runId, redacted);

  // ── Emit compliance audit entry ──────────────────────────────────────────
  const auditEntry = logger.logAction({
    entityId:   runId,
    entityType: DataEntityType.AUDIT_LOG,
    action:     RetentionAction.DELETE,
    actor:      'retention-purge-job',
    details: {
      runId,
      startedAt,
      completedAt,
      totalRowsDeleted,
      tables: redacted,
    },
    compliance: 'GDPR',
    notes: `Automated retention purge run ${runId}`,
  });

  return {
    runId,
    startedAt,
    completedAt,
    totalRowsDeleted,
    tables: tableMetrics,
    auditLogId: auditEntry.id,
  };
}
