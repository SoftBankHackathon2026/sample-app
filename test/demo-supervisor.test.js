const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {once} = require('node:events');
const {setTimeout: delay} = require('node:timers/promises');
const {createClient} = require('redis');
const {
  config,
  authorized,
  CYCLE,
  openState,
} = require('../packages/demo-supervisor');

const TOKEN = 'demo-test-control-token-0000000000000000';
async function wait(check) {
  for (let n = 0; n < 100; n++) {
    try {
      const value = await check();
      if (value) return value;
    } catch {}
    await delay(50);
  }
  throw new Error('condition timed out');
}
async function command(action, cycle, token = TOKEN) {
  const r = await fetch('http://127.0.0.1:9093/demo', {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({action, cycle}),
  });
  return {status: r.status, body: await r.json()};
}
async function processFixture(t, env) {
  const child = spawn(
    process.execPath,
    ['packages/demo-supervisor.js', 'serve'],
    {
      env: {...process.env, ...env},
      stdio: 'ignore',
    },
  );
  async function stop() {
    if (child.exitCode !== null || child.signalCode) return;
    const exit = once(child, 'exit');
    child.kill('SIGTERM');
    await exit;
  }
  t.after(stop);
  await wait(async () => (await command('status')).status === 200);
  return {stop};
}

test('reset is opt-in, roles and token are required, cycle cannot traverse paths', () => {
  assert.throws(() => config({}), /DISABLED/);
  assert.throws(
    () => config({DEMO_RESET_ENABLED: 'true', DEMO_ROLE: 'source'}),
    /TOKEN/,
  );
  assert.equal(authorized('Bearer ' + TOKEN, TOKEN), true);
  assert.equal(authorized('Bearer wrong', TOKEN), false);
  for (const name of ['../x', 'a/b', 'a.b', '', 'A', 'a'.repeat(49)])
    assert.equal(CYCLE.test(name), false);
});

test('bot archives are distinct directories and nonempty cycle reuse is refused', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'demo-state-'));
  t.after(() => fs.rm(root, {recursive: true, force: true}));
  const store = await openState(
    {BOT_LEDGER_DIR: path.join(root, 'legacy')},
    'bot',
  );
  await store.reset('demo_1');
  await store.reset('demo_1'); // interrupted creation of an empty directory
  await fs.writeFile(
    path.join(root, 'demo-cycles/demo_1/ledger.sqlite'),
    'old evidence',
  );
  await assert.rejects(store.reset('demo_1'));
  await store.write({stopped: true, cycle: 'demo_2'});
  assert.deepEqual(await store.read(), {stopped: true, cycle: 'demo_2'});
});

test(
  'real source receipt stops, archives ledger, creates a fresh cycle and stays stopped across restart',
  {skip: !process.env.DEMO_TEST_REDIS_URL},
  async (t) => {
    const redis = createClient({url: process.env.DEMO_TEST_REDIS_URL});
    redis.on('error', () => {});
    await redis.connect();
    t.after(() => redis.quit());
    await redis.flushDb(); // isolated test container only
    const env = {
      DEMO_RESET_ENABLED: 'true',
      DEMO_ROLE: 'source',
      INGRESS_CONTROL_TOKEN: TOKEN,
      REDIS_URL: process.env.DEMO_TEST_REDIS_URL,
      INGRESS_TARGETS: JSON.stringify({
        onprem: {url: 'http://127.0.0.1:19999'},
      }),
    };
    let proc = await processFixture(t, env);
    await wait(() => redis.get('ingress:ledger'));
    const old = await redis.get('ingress:ledger');
    await redis.hSet('ingress:req:fixture', {
      body: 'preserved',
      status: 'accepted',
    });
    assert.equal((await command('stop', null, 'wrong')).status, 401);
    assert.equal((await command('reset', 'demo_1')).status, 409);
    assert.equal((await command('stop')).status, 200);
    assert.equal(await redis.exists('ingress:owner'), 0);
    assert.equal((await command('reset', 'demo_1')).status, 200);
    assert.equal(await redis.get('demo:archive:demo_1:ingress:ledger'), old);
    assert.equal(
      await redis.hGet('demo:archive:demo_1:ingress:req:fixture', 'body'),
      'preserved',
    );
    await proc.stop();
    proc = await processFixture(t, env);
    assert.equal((await command('status')).body.child_running, false);
    assert.equal((await command('start', 'wrong')).status, 409);
    assert.equal((await command('start', 'demo_1')).status, 200);
    await wait(() => redis.get('ingress:ledger'));
    assert.notEqual(await redis.get('ingress:ledger'), old);
    assert.equal(await redis.hGet('ingress:control', 'target'), 'onprem');
    assert.equal(await redis.hGet('ingress:control', 'mode'), 'paused');
    await command('stop');
    await command('reset', 'demo_2');
    assert.equal((await command('start', 'demo_2')).status, 200);
    await wait(() => redis.get('ingress:ledger'));
    await command('stop');
    assert.equal((await command('reset', 'demo_1')).status, 409);
    assert.equal((await command('reset', 'demo_3')).status, 200);
    assert.equal((await command('start', 'demo_3')).status, 200);
    await proc.stop();
  },
);

test('real bot changes its ledger and run ID per cycle without generating requests', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'demo-bot-process-'));
  t.after(() => fs.rm(root, {recursive: true, force: true}));
  const env = {
    DEMO_RESET_ENABLED: 'true',
    DEMO_ROLE: 'bot',
    BOT_CONTROL_TOKEN: TOKEN,
    BOT_LEDGER_DIR: path.join(root, 'legacy'),
    BOT_BASE_URL: 'http://127.0.0.1:19999',
    BOT_RUN_ID: 'legacy',
    BOT_GENERATION: 'stopped',
  };
  const proc = await processFixture(t, env);
  async function snapshot() {
    const response = await fetch('http://127.0.0.1:9091/control', {
      headers: {Authorization: 'Bearer ' + TOKEN},
    });
    return response.ok && response.json();
  }
  const old = await wait(snapshot);
  await command('stop');
  await command('reset', 'bot_demo_1');
  await command('start', 'bot_demo_1');
  const first = await wait(snapshot);
  assert.equal(first.run_id, 'bot_demo_1');
  assert.notEqual(first.ledger_id, old.ledger_id);
  assert.equal(first.generation, 'stopped');
  await command('stop');
  await command('reset', 'bot_demo_2');
  await command('start', 'bot_demo_2');
  const second = await wait(snapshot);
  assert.equal(second.run_id, 'bot_demo_2');
  assert.notEqual(second.ledger_id, first.ledger_id);
  await proc.stop();
});
