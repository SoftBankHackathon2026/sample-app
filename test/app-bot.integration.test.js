const {readLedger} = require('./bot-ledger-reader');
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {mkdtemp, rm, readFile} = require('node:fs/promises');
const {tmpdir} = require('node:os');
const {join} = require('node:path');
const {createServer} = require('node:http');
const {Pool} = require('pg');
const {readFileSync} = require('node:fs');
const {createPostgresRequestStore} = require('@team-ghost/request-store');
const {createTrafficBot, createHttpAdapter} =
    require('@team-ghost/traffic-bot');
const {createApp} = require('../src/app');

const SKIP = !process.env.TEST_DATABASE_URL;
/** The team INSERT worker's run ID; tests must never delete its rows. */
const TEAM_RUN_ID = 'forward_001';

/**
 * Hosts the sample API on a real PG test database for one test. `env`, if
 * given, replaces process.env for the app. Rows of the test run ID and of
 * request IDs pushed to `ownRequestIds` are removed afterwards.
 */
async function hostApi(t, env) {
  const connectionString = process.env.TEST_DATABASE_URL;
  const admin = new Pool({connectionString});
  await admin.query(readFileSync(join(__dirname,
      '../packages/request-store/schema.sql'), 'utf8'));
  const store = createPostgresRequestStore({connectionString});
  const server = createApp({store, ...(env ? {env} : {})})
      .listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  const directory = await mkdtemp(join(tmpdir(), 'ghost-e2e-'));
  const runId = `e2e-${randomUUID()}`;
  const host = {base: `http://127.0.0.1:${server.address().port}`, store,
    admin, directory, runId, bot: null, ownRequestIds: []};
  t.after(async () => {
    if (host.bot) await host.bot.stop();
    await new Promise((resolve) => server.close(resolve));
    await store.close();
    await admin.query('DELETE FROM migration_demo WHERE run_id=$1', [runId]);
    await admin.query(
        'DELETE FROM migration_demo WHERE request_id=ANY($1::uuid[])',
        [host.ownRequestIds]);
    await admin.end();
    await rm(directory, {recursive: true, force: true});
  });
  return host;
}
async function countRows(host) {
  return (await host.admin.query(
      'SELECT count(*)::text AS n FROM migration_demo WHERE run_id=$1',
      [host.runId])).rows[0].n;
}
function syntheticRecord(runId) {
  return {request_id: randomUUID(), run_id: runId, phase: 'onprem_live',
    request_no: '1', payload: 'E2E synthetic data',
    created_at: new Date().toISOString()};
}
/** Forwards to the API but replaces every 2xx POST body after it commits. */
async function breakPostReplies(t, base, replace) {
  const proxy = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) {
      body += chunk;
    }
    const upstream = await fetch(`${base}${req.url}`, {method: req.method,
      headers: {'Content-Type': 'application/json'},
      ...(req.method === 'POST' ? {body} : {})});
    const text = await upstream.text();
    res.writeHead(upstream.status, {'Content-Type': 'application/json'});
    res.end(req.method === 'POST' && upstream.ok ? replace(text) : text);
  });
  proxy.listen(0, '127.0.0.1');
  await new Promise((resolve) => proxy.once('listening', resolve));
  t.after(() => new Promise((resolve) => proxy.close(resolve)));
  return `http://127.0.0.1:${proxy.address().port}`;
}

test('real PG + hosted API + bot reconcile response loss and replay ledger',
    {skip: SKIP}, async (t) => {
      const host = await hostApi(t);
      const adapter = createHttpAdapter({baseUrl: host.base});
      let dropped = false;
      host.bot = await createTrafficBot({ledgerDir: host.directory,
        runId: host.runId, retryDelayMs: 1, adapter: {
          read: (...args) => adapter.read(...args),
          write: async (...args) => {
            const reply = await adapter.write(...args);
            if (!dropped) { dropped = true; throw new Error('lost response'); }
            return reply;
          },
        }});
      const record = syntheticRecord(host.runId);
      await host.bot.enqueue(record);
      await host.bot.processPending();
      assert.equal(host.bot.snapshot().committed, 1);
      assert.equal(await countRows(host), '1');
      await host.bot.stop();
      host.bot = await createTrafficBot({ledgerDir: host.directory,
        runId: host.runId, adapter});
      await host.bot.processPending();
      assert.equal(host.bot.snapshot().committed, 1);
      assert.deepEqual((await host.store.get(record.request_id)).request,
          record);
      await host.store.pauseWrites();
      assert.equal((await fetch(`${host.base}/readyz`)).status, 200);
      assert.equal((await fetch(
          `${host.base}/api/requests/${record.request_id}`)).status, 200);
    });

test('real PG: a committed POST with a broken 2xx reply is settled by GET',
    {skip: SKIP}, async (t) => {
      const replies = {
        'invalid JSON': () => 'not json',
        'different six fields': (text) => {
          const body = JSON.parse(text);
          return JSON.stringify({...body,
            request: {...body.request, payload: 'tampered'}});
        },
      };
      for (const [name, replace] of Object.entries(replies)) {
        await t.test(name, async (t) => {
          const host = await hostApi(t);
          const baseUrl = await breakPostReplies(t, host.base, replace);
          host.bot = await createTrafficBot({ledgerDir: host.directory,
            runId: host.runId, retryDelayMs: 1,
            adapter: createHttpAdapter({baseUrl})});
          await host.bot.enqueue(syntheticRecord(host.runId));
          await host.bot.processPending();
          assert.equal(host.bot.snapshot().committed, 1);
          assert.equal(host.bot.snapshot().failed, 0);
          assert.equal(await countRows(host), '1');
          await host.bot.stop();
          host.bot = null;
          const {entries} = JSON.parse(await readLedger(host.directory));
          assert.equal(entries[0].state, 'committed');
          assert.equal(entries[0].observations.length, 1);
        });
      }
    });

const API_TOKEN = `test-token-${randomUUID()}`;
const ENFORCED_ENV = {REQUEST_API_AUTH: 'required',
  REQUEST_API_TOKEN: API_TOKEN};

function postRecord(base, record, headers = {}) {
  return fetch(`${base}/api/requests`, {method: 'POST',
    headers: {...headers, 'Content-Type': 'application/json'},
    body: JSON.stringify(record)});
}

test('S3 real PG: enforced API refuses an anonymous forward_001 POST',
    {skip: SKIP}, async (t) => {
      const host = await hostApi(t, ENFORCED_ENV);
      const forged = syntheticRecord(TEAM_RUN_ID);
      host.ownRequestIds.push(forged.request_id);
      const anonymous = await postRecord(host.base, forged);
      await anonymous.body?.cancel();
      assert.equal(anonymous.status, 401);
      // Counts the forged ID only; other files may commit forward_001 rows.
      const {rows} = await host.admin.query(
          'SELECT count(*)::text AS n FROM migration_demo WHERE request_id=$1',
          [forged.request_id]);
      assert.equal(rows[0].n, '0');
      const authorized = await postRecord(host.base,
          syntheticRecord(host.runId), {authorization: `Bearer ${API_TOKEN}`});
      assert.equal(authorized.status, 201);
      assert.equal((await authorized.json()).status, 'committed');
      assert.equal(await countRows(host), '1');
    });

test('S3 real PG: enforced API commits bot writes only with the Bearer header',
    {skip: SKIP}, async (t) => {
      await t.test('adapter with the header commits', async (t) => {
        const host = await hostApi(t, ENFORCED_ENV);
        host.bot = await createTrafficBot({ledgerDir: host.directory,
          runId: host.runId, retryDelayMs: 1,
          adapter: createHttpAdapter({baseUrl: host.base,
            headers: {authorization: `Bearer ${API_TOKEN}`}})});
        await host.bot.enqueue(syntheticRecord(host.runId));
        await host.bot.processPending();
        assert.equal(host.bot.snapshot().committed, 1);
        assert.equal(await countRows(host), '1');
        await host.bot.stop();
        host.bot = null;
        const ledger = await readLedger(host.directory);
        assert.ok(!ledger.includes(API_TOKEN));
      });
      await t.test('adapter without the header fails with HTTP_401',
          async (t) => {
            const host = await hostApi(t, ENFORCED_ENV);
            host.bot = await createTrafficBot({ledgerDir: host.directory,
              runId: host.runId, retryDelayMs: 1,
              adapter: createHttpAdapter({baseUrl: host.base})});
            await host.bot.enqueue(syntheticRecord(host.runId));
            await host.bot.processPending();
            assert.equal(host.bot.snapshot().committed, 0);
            assert.equal(host.bot.snapshot().failed, 1);
            await host.bot.stop();
            host.bot = null;
            const {entries} = JSON.parse(await readLedger(host.directory));
            assert.deepEqual(entries.map(({state, lastError}) =>
              [state, lastError]), [['failed', 'HTTP_401']]);
            assert.equal(await countRows(host), '0');
          });
    });

test('bounded generator commits exact rows and stays stopped after restart',
    {skip: SKIP}, async (t) => {
      const host = await hostApi(t);
      const options = {adapter: createHttpAdapter({baseUrl: host.base}),
        ledgerDir: host.directory, runId: host.runId, generation: 'stopped',
        ratePerSecond: 100};
      host.bot = await createTrafficBot(options);
      host.bot.start();
      await host.bot.setGeneration(true, {limit: 3});
      const waitForCommits = async (count) => {
        const deadline = Date.now() + 5000;
        while (host.bot.snapshot().committed < count && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        assert.equal(host.bot.snapshot().committed, count);
      };
      await waitForCommits(3);
      assert.equal(await countRows(host), '3');
      assert.equal(host.bot.snapshot().generation, 'stopped');
      await host.bot.stop();
      host.bot = await createTrafficBot({...options, generation: 'running'});
      host.bot.start();
      await new Promise((resolve) => setTimeout(resolve, 60));
      assert.equal(await countRows(host), '3');
      await host.bot.setGeneration(true, {limit: 5});
      await waitForCommits(5);
      await host.bot.setGeneration(false);
      assert.equal(await countRows(host), '5');
      assert.equal(host.bot.snapshot().pending, 0);
    });
