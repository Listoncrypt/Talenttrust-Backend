/**
 * src/retention/purge.test.ts
 *
 * Tests for the data-retention purge job (Issue #261).
 *
 * The better-sqlite3 native addon requires a native build step that is not
 * available in this environment (Node v24 / no Python for node-gyp).
 * We therefore mock the entire ../db/database module and simulate the DB
 * layer in-process using lightweight JavaScript state.  This lets us test
 * all the business logic without the real driver.
 */

import { ComplianceAuditLogger } from './audit';
import { RetentionAction } from './types';
import {
  runDataRetentionPurge,
  loadPurgeConfig,
  PurgeConfig,
} from './purge';

// ─── Mock DB layer ────────────────────────────────────────────────────────────

/** A row stored in a simulated table. */
interface MockRow {
  rowid: number;
  created_at: string;
}

// Mutable per-test state — reassigned in beforeEach via buildMockDb()
let _mockStore: Record<string, MockRow[]> = {};
let _deletedRowids: Record<string, number[]> = {};
let _auditLogInserts: unknown[][] = [];
let _mockDb: ReturnType<typeof buildMockDb>;

/**
 * Builds a fake Database object whose prepare / transaction / exec methods
 * operate on the module-level _mockStore instead of a real SQLite file.
 */
function buildMockDb() {
  _mockStore = {};
  _deletedRowids = {};
  _auditLogInserts = [];

  return {
    exec: jest.fn(), // CREATE TABLE — no-op in tests

    prepare: jest.fn((sql: string) => {
      const selectMatch = /SELECT rowid FROM\s+(\w+)/i.exec(sql);
      const deleteMatch = /DELETE FROM\s+(\w+)/i.exec(sql);
      const isInsertAudit = /INSERT INTO purge_audit_log/i.test(sql);

      // Batch select: return expired rowids up to `limit`
      if (selectMatch) {
        const table = selectMatch[1];
        return {
          all: jest.fn((cutoff: string, limit: number) => {
            const rows = _mockStore[table] ?? [];
            return rows
              .filter((r) => r.created_at < cutoff)
              .slice(0, limit)
              .map((r) => ({ rowid: r.rowid }));
          }),
        };
      }

      // Delete by rowid: removes the row from the mock store
      if (deleteMatch) {
        const table = deleteMatch[1];
        return {
          run: jest.fn((rowid: number) => {
            if (!_deletedRowids[table]) _deletedRowids[table] = [];
            _deletedRowids[table].push(rowid);
            if (_mockStore[table]) {
              _mockStore[table] = _mockStore[table].filter((r) => r.rowid !== rowid);
            }
          }),
        };
      }

      // Audit log insert: capture args for assertion
      if (isInsertAudit) {
        return {
          run: jest.fn((...args: unknown[]) => {
            _auditLogInserts.push(args);
          }),
        };
      }

      // Fallback — shouldn't be reached in normal flow
      return { all: jest.fn(() => []), run: jest.fn(), get: jest.fn(() => null) };
    }),

    // Wraps fn in an immediately-invocable wrapper (mimics db.transaction)
    transaction: jest.fn(
      (fn: (...a: unknown[]) => unknown) =>
        (...args: unknown[]) =>
          fn(...args),
    ),
  };
}

// Register the mock BEFORE any imports of the module under test execute.
// The arrow function is called lazily so _mockDb is always read at call time.
jest.mock('../db/database', () => ({
  getDb: () => _mockDb,
  closeDb: jest.fn(),
}));

// ─── Seed helper ─────────────────────────────────────────────────────────────

const ONE_DAY_MS = 86_400_000;

/** ISO-8601 timestamp offset from now by `deltaMs`. Negative = past. */
function isoAt(deltaMs: number): string {
  return new Date(Date.now() + deltaMs).toISOString();
}

/** Pre-populate a simulated table with the given rows. */
function seedTable(table: string, rows: MockRow[]): void {
  _mockStore[table] = [...rows];
}

/** Tightly scoped purge config for tests: 30-day windows, 100-row batches. */
const TEST_CONFIG: PurgeConfig = {
  eventsDays:   30,
  webhooksDays: 30,
  dlqDays:      30,
  batchSize:    100,
};

// ─── loadPurgeConfig() ───────────────────────────────────────────────────────

describe('loadPurgeConfig()', () => {
  afterEach(() => {
    delete process.env['RETENTION_EVENTS_DAYS'];
    delete process.env['RETENTION_WEBHOOKS_DAYS'];
    delete process.env['RETENTION_DLQ_DAYS'];
  });

  it('returns defaults when env vars are absent', () => {
    const cfg = loadPurgeConfig();
    expect(cfg.eventsDays).toBe(90);
    expect(cfg.webhooksDays).toBe(30);
    expect(cfg.dlqDays).toBe(7);
    expect(cfg.batchSize).toBe(100);
  });

  it('parses valid env overrides', () => {
    process.env['RETENTION_EVENTS_DAYS']   = '14';
    process.env['RETENTION_WEBHOOKS_DAYS'] = '7';
    process.env['RETENTION_DLQ_DAYS']      = '3';
    const cfg = loadPurgeConfig();
    expect(cfg.eventsDays).toBe(14);
    expect(cfg.webhooksDays).toBe(7);
    expect(cfg.dlqDays).toBe(3);
  });

  it('throws on a non-numeric override', () => {
    process.env['RETENTION_EVENTS_DAYS'] = 'banana';
    expect(() => loadPurgeConfig()).toThrow('RETENTION_EVENTS_DAYS');
  });

  it('throws on a zero/negative override', () => {
    process.env['RETENTION_EVENTS_DAYS'] = '0';
    expect(() => loadPurgeConfig()).toThrow('RETENTION_EVENTS_DAYS');
  });
});

// ─── events table ─────────────────────────────────────────────────────────────

describe('runDataRetentionPurge() — events table', () => {
  let auditLogger: ComplianceAuditLogger;

  beforeEach(() => {
    _mockDb = buildMockDb();
    auditLogger = new ComplianceAuditLogger();
  });

  it('deletes expired rows and keeps live rows intact', async () => {
    seedTable('event_processing_audit', [
      { rowid: 1, created_at: isoAt(-40 * ONE_DAY_MS) }, // expired
      { rowid: 2, created_at: isoAt(-40 * ONE_DAY_MS) }, // expired
      { rowid: 3, created_at: isoAt(-40 * ONE_DAY_MS) }, // expired
      { rowid: 4, created_at: isoAt(-10 * ONE_DAY_MS) }, // live
      { rowid: 5, created_at: isoAt(-10 * ONE_DAY_MS) }, // live
    ]);
    seedTable('webhook_dlq', []);
    seedTable('purge_audit_log', []);

    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    const eventsMetrics = result.tables.find((t) => t.table === 'event_processing_audit')!;
    expect(eventsMetrics.rowsDeleted).toBe(3);
    // Only 2 live rows remain
    expect(_mockStore['event_processing_audit']).toHaveLength(2);
  });

  it('does nothing when all rows are within the retention window', async () => {
    seedTable('event_processing_audit', [
      { rowid: 1, created_at: isoAt(-5 * ONE_DAY_MS) },
      { rowid: 2, created_at: isoAt(-1 * ONE_DAY_MS) },
    ]);
    seedTable('webhook_dlq', []);
    seedTable('purge_audit_log', []);

    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    const eventsMetrics = result.tables.find((t) => t.table === 'event_processing_audit')!;
    expect(eventsMetrics.rowsDeleted).toBe(0);
    expect(eventsMetrics.batchesRun).toBe(0);
    expect(_mockStore['event_processing_audit']).toHaveLength(2);
  });
});

// ─── webhook_dlq table ────────────────────────────────────────────────────────

describe('runDataRetentionPurge() — webhook_dlq table', () => {
  let auditLogger: ComplianceAuditLogger;

  beforeEach(() => {
    _mockDb = buildMockDb();
    auditLogger = new ComplianceAuditLogger();
    seedTable('event_processing_audit', []);
    seedTable('purge_audit_log', []);
  });

  it('deletes expired webhook DLQ entries and keeps recent ones', async () => {
    seedTable('webhook_dlq', [
      { rowid: 1, created_at: isoAt(-60 * ONE_DAY_MS) }, // expired
      { rowid: 2, created_at: isoAt(-35 * ONE_DAY_MS) }, // expired
      { rowid: 3, created_at: isoAt(-2  * ONE_DAY_MS) }, // live
    ]);

    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    const webhookMetrics = result.tables.find((t) => t.table === 'webhook_dlq')!;
    expect(webhookMetrics.rowsDeleted).toBe(2);
    expect(_mockStore['webhook_dlq']).toHaveLength(1);
  });

  it('handles an empty DLQ table gracefully', async () => {
    seedTable('webhook_dlq', []);

    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    const webhookMetrics = result.tables.find((t) => t.table === 'webhook_dlq')!;
    expect(webhookMetrics.rowsDeleted).toBe(0);
    expect(webhookMetrics.batchesRun).toBe(0);
  });
});

// ─── audit trail ─────────────────────────────────────────────────────────────

describe('runDataRetentionPurge() — audit trail', () => {
  let auditLogger: ComplianceAuditLogger;

  beforeEach(() => {
    _mockDb = buildMockDb();
    auditLogger = new ComplianceAuditLogger();
    seedTable('event_processing_audit', [{ rowid: 1, created_at: isoAt(-90 * ONE_DAY_MS) }]);
    seedTable('webhook_dlq',            [{ rowid: 1, created_at: isoAt(-90 * ONE_DAY_MS) }]);
    seedTable('purge_audit_log', []);
  });

  it('inserts one purge_audit_log row per table processed', async () => {
    await runDataRetentionPurge(TEST_CONFIG, auditLogger);
    // 3 tables (events, webhook_dlq, purge_audit_log self-prune)
    expect(_auditLogInserts).toHaveLength(3);
  });

  it('stores a non-empty HMAC proof on each audit insert', async () => {
    await runDataRetentionPurge(TEST_CONFIG, auditLogger);
    for (const args of _auditLogInserts) {
      // args layout: [id, run_id, table_name, cutoff_iso, batches_run,
      //               rows_deleted, duration_ms, hmac_proof, created_at]
      const hmacProof = args[7] as string;
      expect(typeof hmacProof).toBe('string');
      expect(hmacProof.length).toBeGreaterThan(0);
    }
  });

  it('emits a ComplianceAuditLogger DELETE entry with GDPR compliance tag', async () => {
    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    expect(auditLogger.getLogCount()).toBeGreaterThanOrEqual(1);
    const log = auditLogger.getLogById(result.auditLogId);
    expect(log).toBeDefined();
    expect(log!.action).toBe(RetentionAction.DELETE);
    expect(log!.actor).toBe('retention-purge-job');
    expect(log!.compliance).toBe('GDPR');
    expect(log!.proof).toBeDefined();
    expect(log!.proof!.length).toBeGreaterThan(0);
  });

  it('returns a well-formed PurgeRunResult', async () => {
    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);

    expect(typeof result.runId).toBe('string');
    expect(result.runId.length).toBeGreaterThan(0);
    expect(typeof result.startedAt).toBe('string');
    expect(typeof result.completedAt).toBe('string');
    expect(typeof result.totalRowsDeleted).toBe('number');
    expect(result.tables).toHaveLength(3);
    expect(typeof result.auditLogId).toBe('string');
  });
});

// ─── batch processing ─────────────────────────────────────────────────────────

describe('runDataRetentionPurge() — batch processing', () => {
  let auditLogger: ComplianceAuditLogger;

  beforeEach(() => {
    _mockDb = buildMockDb();
    auditLogger = new ComplianceAuditLogger();
    seedTable('webhook_dlq', []);
    seedTable('purge_audit_log', []);
  });

  it('runs multiple batches when expired row count exceeds batchSize', async () => {
    // 250 expired → 100 + 100 + 50 = 3 batches
    const rows: MockRow[] = Array.from({ length: 250 }, (_, i) => ({
      rowid: i + 1,
      created_at: isoAt(-60 * ONE_DAY_MS),
    }));
    seedTable('event_processing_audit', rows);

    const result = await runDataRetentionPurge({ ...TEST_CONFIG, batchSize: 100 }, auditLogger);

    const eventsMetrics = result.tables.find((t) => t.table === 'event_processing_audit')!;
    expect(eventsMetrics.rowsDeleted).toBe(250);
    expect(eventsMetrics.batchesRun).toBe(3);
    expect(_mockStore['event_processing_audit']).toHaveLength(0);
  });

  it('sums totalRowsDeleted correctly across all tables', async () => {
    seedTable('event_processing_audit', [
      { rowid: 1, created_at: isoAt(-50 * ONE_DAY_MS) },
    ]);
    seedTable('webhook_dlq', [
      { rowid: 1, created_at: isoAt(-50 * ONE_DAY_MS) },
      { rowid: 2, created_at: isoAt(-50 * ONE_DAY_MS) },
    ]);

    const result = await runDataRetentionPurge(TEST_CONFIG, auditLogger);
    // 1 event + 2 webhooks + 0 dlq self-prune = 3
    expect(result.totalRowsDeleted).toBe(3);
  });
});
