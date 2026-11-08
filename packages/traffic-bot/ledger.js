const fs = require('node:fs/promises');
const path = require('node:path');
const {DatabaseSync} = require('node:sqlite');
const {randomUUID} = require('node:crypto');
const {acquireOwnerLock} = require('./owner-lock');

// Limits cover retained evidence; completed records are not polled or rewritten.
const DEFAULT_MAX_RECORDS = 10000;
const DEFAULT_MAX_LEDGER_BYTES = 8 * 1024 * 1024;

function positiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

/**
 * Open one durable, bounded journal. One process may own a directory; runId
 * is written to the owner lock for diagnostics only.
 */
async function openLedger({dir, maxPending = 1000,
  maxRecords = DEFAULT_MAX_RECORDS, maxLedgerBytes = DEFAULT_MAX_LEDGER_BYTES,
  runId = null} = {}) {
  if (typeof dir !== 'string' || !dir) {
    throw new TypeError('ledger dir is required');
  }
  positiveInteger(maxPending, 'maxPending');
  positiveInteger(maxRecords, 'maxRecords');
  positiveInteger(maxLedgerBytes, 'maxLedgerBytes');
  await fs.mkdir(dir, {recursive: true, mode: 0o700});
  const lock = await acquireOwnerLock(dir, {runId});
  const filename = path.join(dir, 'ledger.json');
  let data;
  let db;
  let closed = false;
  let queue = Promise.resolve();
  try {
    // Exclusive lock is acquired before cleaning snapshots left by a crash.
    for (const name of await fs.readdir(dir)) {
      if (/^\.ledger-[0-9a-f-]+\.tmp$/.test(name)) {
        await fs.rm(path.join(dir, name), {force: true});
      }
    }
    const dbPath = path.join(dir, 'ledger.sqlite');
    db = new DatabaseSync(dbPath);
    await fs.chmod(dbPath, 0o600);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=128;');
    db.exec('CREATE TABLE IF NOT EXISTS entries (id TEXT PRIMARY KEY, body TEXT NOT NULL); CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);');
    const initialized = db.prepare("SELECT value FROM metadata WHERE key='version'").get();
    if (initialized && initialized.value !== '2') throw new Error('unsupported ledger version');
    const stat = await fs.stat(filename).catch((error) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    if (!initialized && stat && stat.size > maxLedgerBytes) {
      throw new Error('ledger size limit exceeded');
    }
    data = initialized ? {version: 1, entries: db.prepare('SELECT body FROM entries ORDER BY rowid').all().map(r => JSON.parse(r.body))} : stat ? JSON.parse(await fs.readFile(filename, 'utf8')) : {version: 1, entries: []};
    if (data.version !== 1 || !Array.isArray(data.entries)) {
      throw new Error('unsupported or corrupt ledger');
    }
    if (data.entries.length > maxRecords) {
      throw new Error('ledger record limit exceeded');
    }
    const ids = new Set();
    for (const entry of data.entries) {
      if (!entry.request || typeof entry.request.request_id !== 'string' ||
          ids.has(entry.request.request_id) ||
          !['pending', 'committed', 'failed', 'unknown']
              .includes(entry.state) ||
          !Number.isSafeInteger(entry.attempts) || entry.attempts < 0 ||
          !Number.isSafeInteger(entry.enqueuedAt) || entry.enqueuedAt < 0 ||
          !Number.isSafeInteger(entry.nextAttemptAt) ||
          entry.nextAttemptAt < 0 ||
          (entry.acceptedAt !== undefined &&
            (!Number.isSafeInteger(entry.acceptedAt) || entry.acceptedAt < 0))) {
        throw new Error('invalid ledger entry');
      }
      ids.add(entry.request.request_id);
    }
    if (!initialized) {
      db.exec('BEGIN IMMEDIATE');
      try {
        const insert = db.prepare('INSERT INTO entries(id,body) VALUES(?,?)');
        for (const e of data.entries) insert.run(e.request.request_id, JSON.stringify(e));
        db.prepare("INSERT INTO metadata VALUES('version','2')").run();
        db.exec('COMMIT');
      } catch (e) { db.exec('ROLLBACK'); throw e; }
    }
    const identity = db.prepare(
        "SELECT value FROM metadata WHERE key='ledger_id'").get();
    if (!identity) {
      db.prepare('INSERT INTO metadata(key,value) VALUES(?,?)')
          .run('ledger_id', JSON.stringify(randomUUID()));
    } else if (!/^[0-9a-f-]{36}$/.test(JSON.parse(identity.value))) {
      throw new Error('invalid ledger identity');
    }
    // Prevent old binaries from silently reading stale JSON after conversion.
    // Backup and marker are durable before the first new SQLite mutation.
    if (stat) {
      const old = await fs.readFile(filename, 'utf8');
      if (JSON.parse(old).version === 1) {
        const backup = path.join(dir, 'ledger.v1.backup.json');
        try { await fs.writeFile(backup, old, {flag: 'wx', mode: 0o600}); }
        catch (e) {
          if (e.code !== 'EEXIST' || await fs.readFile(backup, 'utf8') !== old) throw e;
        }
        const h = await fs.open(backup, 'r');
        try { await h.sync(); } finally { await h.close(); }
      }
    }
    const marker = path.join(dir, `.ledger-${randomUUID()}.tmp`);
    const h = await fs.open(marker, 'wx', 0o600);
    try {
      await h.writeFile(JSON.stringify({version: 2, storage: 'ledger.sqlite'}));
      await h.sync();
    } finally { await h.close(); }
    await lock.assertHeld();
    await fs.rename(marker, filename);
    // Commit the directory entry too before acknowledging any request.
    const directory = await fs.open(dir, 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } catch (error) {
    if (db) db.close();
    await lock.release();
    throw error;
  }

  const records = new Map(data.entries.map(e => [e.request.request_id, e]));
  const pending = new Map();
  const counts = {pending: 0, accepted_pending: 0, committed: 0, failed: 0, unknown: 0};
  let bytes = Buffer.byteLength(JSON.stringify({version: 1, entries: []}));
  for (const e of records.values()) {
    bytes += Buffer.byteLength(JSON.stringify(e)) + 1;
    counts[e.state]++;
    if (e.state === 'pending') {
      pending.set(e.request.request_id, e);
      if (e.acceptedAt) counts.accepted_pending++;
    }
  }
  let recordCount = records.size;
  records.clear();
  data = null;
  const find = db.prepare('SELECT body FROM entries WHERE id=?');
  const get = id => { const row = find.get(id); return row ? JSON.parse(row.body) : undefined; };
  if (bytes > maxLedgerBytes) {
    db.close(); await lock.release(); throw new Error('ledger size limit exceeded');
  }
  const metadataGet = db.prepare('SELECT value FROM metadata WHERE key=?');
  const metadataSet = db.prepare(
      'INSERT INTO metadata(key,value) VALUES(?,?) ' +
      'ON CONFLICT(key) DO UPDATE SET value=excluded.value');
  const save = db.prepare('INSERT INTO entries(id,body) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body');
  function checkOpen() {
    if (closed) throw new Error('ledger is closed');
  }
  function mutate(make) {
    const operation = queue.then(async () => {
      checkOpen();
      const entry = make();
      const id = entry.request.request_id;
      const previous = get(id);
      const body = JSON.stringify(entry);
      const nextBytes = bytes + Buffer.byteLength(body) + (previous ?
        -Buffer.byteLength(JSON.stringify(previous)) : 1);
      if (nextBytes > maxLedgerBytes) throw new Error('ledger size limit exceeded');
      await lock.assertHeld();
      // SQLite commits only this row, with FULL durability, before publishing
      // state to the delivery loop. A failed transaction leaves views unchanged.
      save.run(id, body);
      if (previous) {
        counts[previous.state]--;
        if (previous.state === 'pending' && previous.acceptedAt) counts.accepted_pending--;
      }
      counts[entry.state]++;
      if (!previous) recordCount++;
      pending.delete(id);
      if (entry.state === 'pending') {
        pending.set(id, entry);
        if (entry.acceptedAt) counts.accepted_pending++;
      }
      bytes = nextBytes;
    });
    queue = operation.catch(() => {});
    return operation;
  }
  return {
    entries() { return db.prepare('SELECT body FROM entries ORDER BY rowid').all().map(r => JSON.parse(r.body)); },
    summary() { return {...counts}; },
    metadata(key) {
      checkOpen();
      const row = metadataGet.get(key);
      return row ? JSON.parse(row.value) : undefined;
    },
    setMetadata(key, value) {
      const body = JSON.stringify(value);
      const operation = queue.then(async () => {
        checkOpen();
        await lock.assertHeld();
        metadataSet.run(key, body);
      });
      queue = operation.catch(() => {});
      return operation;
    },
    pendingEntries() { return structuredClone([...pending.values()]); },
    add(request) {
      return mutate(() => {
        if (find.get(request.request_id)) throw new Error('request ID already exists in ledger');
        if (recordCount >= maxRecords) throw new Error('ledger record limit exceeded');
        if (pending.size >= maxPending) {
          const error = new Error('ledger pending queue limit exceeded');
          error.code = 'PENDING_LIMIT'; throw error;
        }
        return {request: structuredClone(request), state: 'pending', attempts: 0,
          enqueuedAt: Date.now(), nextAttemptAt: 0, observations: []};
      });
    },
    update(id, changes) {
      return mutate(() => {
        const entry = get(id);
        if (!entry) throw new Error('unknown request ID');
        for (const key of Object.keys(changes)) {
          if (!['state', 'attempts', 'nextAttemptAt', 'completedAt',
            'lastError', 'observations', 'acceptedAt'].includes(key)) throw new Error('immutable ledger field');
        }
        return {...entry, ...structuredClone(changes)};
      });
    },
    async close() {
      if (closed) return;
      await queue; closed = true;
      try { db.close(); } finally { await lock.release(); }
    },
  };
}

module.exports = {openLedger, positiveInteger, DEFAULT_MAX_RECORDS,
  DEFAULT_MAX_LEDGER_BYTES};
