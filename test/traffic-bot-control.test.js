// Generation on/off: the core switch and the CLI control routes. The ledger
// lock needs Linux flock(1); run in the pinned node:22 Linux image.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const {randomUUID} = require('node:crypto');
const {once} = require('node:events');
const {spawn} = require('node:child_process');
const {createServer} = require('node:http');
const {setTimeout: sleep} = require('node:timers/promises');
const {createTrafficBot, createHttpAdapter} = require('../packages/traffic-bot');
const {readControlToken, readInitialGeneration} =
  require('../packages/traffic-bot/control-api');

const RUN_ID = 'control-test';
const BIN = path.resolve(__dirname, '../packages/traffic-bot/bin/traffic-bot.js');

function envelope(request) {
  return {status: 'committed', request, environment: 'local',
    database: {name: 'migration_demo'}};
}

async function fixture(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'ghost-control-'));
  t.after(() => fs.rm(dir, {recursive: true, force: true}));
  return dir;
}

/** Commits every POST and answers reads; counts POSTs. */
async function serveStore(t) {
  const stored = new Map();
  const counts = {posts: 0};
  const server = createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.method === 'POST') {
      let body = '';
      for await (const chunk of req) body += chunk;
      const request = JSON.parse(body);
      stored.set(request.request_id, request);
      counts.posts++;
      res.writeHead(201);
      return res.end(JSON.stringify(envelope(request)));
    }
    const request = stored.get(decodeURIComponent(req.url.split('/').pop()));
    res.writeHead(request ? 200 : 404);
    res.end(JSON.stringify(request ? envelope(request) : {}));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => server.close());
  return {url: `http://127.0.0.1:${server.address().port}`, counts};
}

async function freePort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const {port} = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function until(check, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return true;
    await sleep(25);
  }
  return false;
}

test('a stopped generator makes no requests; starting and stopping it switch only generation',
    async (t) => {
      const dir = await fixture(t);
      const store = await serveStore(t);
      const bot = await createTrafficBot({ledgerDir: dir, runId: RUN_ID,
        ratePerSecond: 50, generation: 'stopped',
        adapter: createHttpAdapter({baseUrl: store.url})});
      t.after(() => bot.stop());
      bot.start();
      await sleep(300);
      assert.equal(store.counts.posts, 0);
      assert.equal(bot.snapshot().generation, 'stopped');
      assert.match(bot.metrics(), /traffic_bot_generating 0/);

      assert.equal(bot.setGeneration(true), 'running');
      assert.ok(await until(() => bot.snapshot().committed >= 3));

      bot.setGeneration(false);
      // Requests already in the ledger still settle after generation stops.
      assert.ok(await until(() => bot.snapshot().pending === 0));
      const settled = store.counts.posts;
      await sleep(300);
      assert.equal(store.counts.posts, settled);
      assert.throws(() => bot.setGeneration('yes'), TypeError);
    });

test('generation option and env values are validated', async (t) => {
  const dir = await fixture(t);
  await assert.rejects(createTrafficBot({ledgerDir: dir, runId: RUN_ID,
    generation: 'paused', adapter: createHttpAdapter({baseUrl: 'http://127.0.0.1:1'})}),
  TypeError);
  assert.equal(readInitialGeneration({}), 'running');
  assert.equal(readInitialGeneration({BOT_GENERATION: 'stopped'}), 'stopped');
  assert.throws(() => readInitialGeneration({BOT_GENERATION: 'off'}),
      {code: 'BOT_GENERATION_INVALID'});
  assert.equal(readControlToken({}), undefined);
  assert.throws(() => readControlToken({BOT_CONTROL_TOKEN: 'short'}),
      {code: 'BOT_CONTROL_TOKEN_INVALID'});
});

async function runCli(t, extraEnv) {
  const dir = await fixture(t);
  const store = await serveStore(t);
  const port = await freePort();
  const child = spawn(process.execPath, [BIN], {stdio: ['ignore', 'ignore', 'pipe'],
    env: {...process.env, BOT_BASE_URL: store.url, BOT_LEDGER_DIR: dir,
      BOT_RUN_ID: RUN_ID, BOT_RATE: '50', BOT_METRICS_PORT: String(port),
      BOT_METRICS_HOST: '127.0.0.1', ...extraEnv}});
  t.after(() => child.kill('SIGTERM'));
  const base = `http://127.0.0.1:${port}`;
  assert.ok(await until(async () => {
    try {
      return (await fetch(`${base}/healthz`)).ok;
    } catch {
      return false;
    }
  }), 'bot did not start');
  return {base, store};
}

test('CLI control routes: read, bearer-protected switch, closed without a token',
    async (t) => {
      const token = `ctl-${randomUUID()}-${randomUUID()}`;
      const {base, store} = await runCli(t, {BOT_GENERATION: 'stopped',
        BOT_CONTROL_TOKEN: token});
      const view = await (await fetch(`${base}/control`)).json();
      assert.equal(view.generation, 'stopped');
      assert.equal(view.run_id, RUN_ID);
      assert.equal(view.rate_per_second, 50);

      const post = (headers, body) => fetch(`${base}/control/generation`,
          {method: 'POST', headers, body});
      assert.equal((await post({}, '{"enabled":true}')).status, 401);
      assert.equal((await post({authorization: 'Bearer wrong'},
          '{"enabled":true}')).status, 401);
      assert.equal((await post({authorization: `Bearer ${token}`},
          '{"enabled":"yes"}')).status, 400);
      assert.equal((await fetch(`${base}/control/generation`)).status, 405);
      assert.equal(store.counts.posts, 0);

      const started = await post({authorization: `Bearer ${token}`}, '{"enabled":true}');
      assert.equal(started.status, 200);
      assert.equal((await started.json()).generation, 'running');
      assert.ok(await until(() => store.counts.posts >= 3));
      assert.match(await (await fetch(`${base}/metrics`)).text(), /traffic_bot_generating 1/);
    });

test('CLI without BOT_CONTROL_TOKEN keeps generating and refuses the switch',
    async (t) => {
      const {base, store} = await runCli(t, {});
      assert.equal((await (await fetch(`${base}/control`)).json()).generation, 'running');
      const res = await fetch(`${base}/control/generation`, {method: 'POST',
        body: '{"enabled":false}'});
      assert.equal(res.status, 403);
      assert.ok(await until(() => store.counts.posts >= 2));
    });
