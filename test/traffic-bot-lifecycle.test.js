const {test} = require('node:test');
const assert = require('node:assert/strict');
const {mkdtemp, rm} = require('node:fs/promises');
const {tmpdir} = require('node:os');
const {join} = require('node:path');
const {setTimeout: sleep} = require('node:timers/promises');
const {createTrafficBot} = require('../packages/traffic-bot');

async function fixture(t) {
  const ledgerDir = await mkdtemp(join(tmpdir(), 'bot-lifecycle-'));
  let bot;
  const stored = new Map();
  const config = {ledgerDir, runId: 'bounded-demo', generation: 'stopped',
    ratePerSecond: 200, retryDelayMs: 1,
    adapter: {
      write: async (request) => {
        stored.set(request.request_id, request);
        return {status: 'committed', request, environment: 'test',
          database: {name: 'migration_demo'}};
      },
      read: async (id) => {
        const request = stored.get(id);
        return request ? {status: 'committed', request, environment: 'test',
          database: {name: 'migration_demo'}} : null;
      },
    }};
  t.after(async () => {
    await bot?.stop();
    await rm(ledgerDir, {recursive: true, force: true});
  });
  return {config, stored, open: async (extra = {}) => {
    bot = await createTrafficBot({...config, ...extra});
    return bot;
  }};
}

async function until(check) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (check()) return;
    await sleep(10);
  }
  assert.fail('condition did not settle');
}

test('bounded batches stop exactly and remain stopped after restart', async (t) => {
  const f = await fixture(t);
  let bot = await f.open();
  const ledgerId = bot.snapshot().ledger_id;
  assert.equal(typeof ledgerId, 'string');
  bot.start();
  await bot.setGeneration(true, {limit: 3});
  await until(() => bot.snapshot().committed === 3);
  assert.equal(bot.snapshot().generated_total, 3);
  assert.equal(bot.snapshot().generation, 'stopped');
  assert.equal(bot.snapshot().generation_limit, 3);
  await sleep(40);
  assert.equal(f.stored.size, 3);
  await bot.stop();
  // The durable stop overrides an accidentally permissive restart setting.
  bot = await f.open({generation: 'running'});
  assert.equal(bot.snapshot().ledger_id, ledgerId);
  assert.equal(bot.snapshot().generated_total, 3);
  bot.start();
  await sleep(40);
  assert.equal(f.stored.size, 3);
  await bot.setGeneration(true, {limit: 3});
  await sleep(30);
  assert.equal(f.stored.size, 3, 'retrying a finished batch adds no requests');
  await bot.setGeneration(true, {limit: 5});
  await until(() => bot.snapshot().committed === 5);
  assert.equal(bot.snapshot().generation, 'stopped');
  assert.equal(bot.snapshot().pending, 0);
});

test('stop acknowledgement excludes later generation and persists', async (t) => {
  const f = await fixture(t);
  let bot = await f.open({generation: 'running', ratePerSecond: 1000});
  bot.start();
  await until(() => bot.snapshot().generated_total >= 2);
  await bot.setGeneration(false);
  const count = bot.snapshot().generated_total;
  await until(() => bot.snapshot().pending === 0);
  await sleep(30);
  assert.equal(bot.snapshot().generated_total, count);
  await bot.stop();
  bot = await f.open({generation: 'running'});
  assert.equal(bot.snapshot().generation, 'stopped');
  assert.equal(bot.snapshot().generated_total, count);
});

test('invalid or stale limits never change the durable generation setting',
    async (t) => {
      const f = await fixture(t);
      const bot = await f.open();
      bot.start();
      for (const limit of [-1, 1.5, '4', null, Number.MAX_SAFE_INTEGER + 1]) {
        await assert.rejects(bot.setGeneration(true, {limit}), TypeError);
        assert.equal(bot.snapshot().generation, 'stopped');
      }
      await bot.setGeneration(true, {limit: 2});
      await until(() => bot.snapshot().generated_total === 2);
      await assert.rejects(bot.setGeneration(true, {limit: 1}),
          {code: 'GENERATION_LIMIT_BELOW_GENERATED'});
      assert.equal(bot.snapshot().generation_limit, 2);
    });

test('failed ledger identity initialization releases the owner lock', async (t) => {
  const {DatabaseSync} = require('node:sqlite');
  const {openLedger} = require('../packages/traffic-bot/ledger');
  const f = await fixture(t);
  const originalPrepare = DatabaseSync.prototype.prepare;
  const prepare = t.mock.method(DatabaseSync.prototype, 'prepare',
      function(statement) {
        const prepared = originalPrepare.call(this, statement);
        if (statement === 'INSERT INTO metadata(key,value) VALUES(?,?)') {
          return {run: () => { throw new Error('simulated disk full'); }};
        }
        return prepared;
      });
  await assert.rejects(openLedger({dir: f.config.ledgerDir}), /disk full/);
  prepare.mock.restore();
  const ledger = await openLedger({dir: f.config.ledgerDir});
  await ledger.close();
});

test('crash after the final request recovers the exhausted budget as stopped',
    async (t) => {
      const {DatabaseSync} = require('node:sqlite');
      const f = await fixture(t);
      let bot = await f.open();
      bot.start();
      await bot.setGeneration(true, {limit: 2});
      await until(() => bot.snapshot().committed === 2);
      await bot.stop();
      // Emulate the crash window after a durable last request, before the
      // following metadata write has persisted the automatic stop.
      const db = new DatabaseSync(join(f.config.ledgerDir, 'ledger.sqlite'));
      db.prepare('UPDATE metadata SET value=? WHERE key=?').run(
          JSON.stringify({enabled: true, limit: 2}),
          `generation_control:${f.config.runId}`);
      db.close();
      bot = await f.open({generation: 'running'});
      assert.equal(bot.snapshot().generated_total, 2);
      assert.equal(bot.snapshot().generation, 'stopped');
      bot.start();
      await sleep(40);
      assert.equal(f.stored.size, 2);
    });
