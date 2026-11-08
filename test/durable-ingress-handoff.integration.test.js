const {test} = require('node:test');
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {once} = require('node:events');
const express = require('express');
const {createClient} = require('redis');
const {createIngress} = require('../src/durable-ingress/service');

const enabled = Boolean(process.env.TEST_HANDOFF_SOURCE_REDIS_URL &&
  process.env.TEST_HANDOFF_TARGET_REDIS_URL);

async function fixture(t) {
  const source = createClient({url: process.env.TEST_HANDOFF_SOURCE_REDIS_URL});
  const target = createClient({url: process.env.TEST_HANDOFF_TARGET_REDIS_URL});
  await Promise.all([source.connect(), target.connect()]);
  await Promise.all([source.flushDb(), target.flushDb()]);
  const rows = new Map();
  const backend = express();
  backend.use(express.json({limit: '128kb'}));
  backend.post('/api/requests', (req, res) => {
    const old = rows.get(req.body.request_id);
    if (old && JSON.stringify(old) !== JSON.stringify(req.body)) {
      return res.status(409).json({error: 'CONFLICT'});
    }
    rows.set(req.body.request_id, req.body);
    res.json({status: 'committed'});
  });
  backend.get('/api/requests/:id', (req, res) => {
    const request = rows.get(req.params.id);
    return request ? res.json({status: 'committed', request}) :
      res.status(404).json({error: 'NOT_FOUND'});
  });
  const host = backend.listen(0);
  await once(host, 'listening');
  const targets = {onprem: {url: `http://127.0.0.1:${host.address().port}`},
    aws_eks: {url: `http://127.0.0.1:${host.address().port}`}};
  const processes = [];
  async function start(redis, extra = {}) {
    const app = await createIngress({redis, targets, controlToken: 'control',
      ...extra});
    const api = app.api.listen(0);
    const control = app.control.listen(0);
    await Promise.all([once(api, 'listening'), once(control, 'listening')]);
    const item = {app, api, control,
      url: `http://127.0.0.1:${api.address().port}`,
      close: async () => {
        await app.close();
        await Promise.all([new Promise((r) => api.close(r)),
          new Promise((r) => control.close(r))]);
      }};
    processes.push(item);
    return item;
  }
  const call = async (server, path, body, token = 'control') => {
    const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, {
      method: body === undefined ? 'GET' : 'POST',
      headers: {'Content-Type': 'application/json',
        ...(token ? {Authorization: `Bearer ${token}`} : {})},
      ...(body === undefined ? {} : {body: JSON.stringify(body)}),
    });
    return {status: response.status, body: await response.json()};
  };
  t.after(async () => {
    for (const item of processes) {
      if (item.api.listening) await item.close();
    }
    await new Promise((r) => host.close(r));
    await Promise.all([source.quit(), target.quit()]);
  });
  return {source, target, start, call, rows};
}

function request(number, payload = 'handoff') {
  return {request_id: randomUUID(), run_id: 'handoff', phase: 'source',
    request_no: String(number), payload, created_at: new Date().toISOString()};
}

async function until(fn) {
  for (let i = 0; i < 200; i++) {
    if (await fn()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail('condition did not settle');
}

test('sealed ledger transfers pending and committed records, survives ' +
  'restart, and forwards stale clients',
  {skip: !enabled}, async (t) => {
    const f = await fixture(t);
    let destination = await f.start(f.target, {standby: true});
    let source = await f.start(f.source, {forwardTargets: {
      aws_receipt: {url: destination.url}}});
    const committed = request(1);
    assert.equal((await f.call(source.api, '/api/requests', committed)).status, 202);
    await f.call(source.control, '/resume', {expected_epoch: '0'});
    await until(async () => (await f.call(source.api,
        '/api/requests/' + committed.request_id)).body.status === 'committed');
    await f.call(source.control, '/pause', {});
    const pending = request(2, '\"\\\n'.repeat(1000));
    await f.call(source.api, '/api/requests', pending);
    const destIdentity = (await f.call(destination.control, '/handoff/status')).body;
    const seal = await f.call(source.control, '/handoff/seal', {
      id: 'migration-one', destination_ledger_id: destIdentity.ledger_id});
    assert.equal(seal.status, 200);
    assert.equal(seal.body.manifest.count, 2);
    assert.equal((await f.call(source.api, '/api/requests', request(3))).status, 503);
    assert.equal((await f.call(source.control, '/resume', {expected_epoch: '0'})).status, 409);
    const page = await f.call(source.control,
        '/handoff/export?id=migration-one&cursor=0-0');
    assert.equal(page.body.done, true);
    assert.equal((await f.call(destination.control, '/handoff/import', page.body)).status, 200);
    assert.equal((await f.call(destination.control, '/handoff/import', page.body)).status, 200);
    await destination.close();
    destination = await f.start(f.target, {standby: true});
    const activation = {...seal.body, target: 'aws_eks'};
    assert.equal((await f.call(destination.control, '/handoff/activate', activation)).status, 200);
    assert.equal((await f.call(destination.control, '/handoff/activate', activation)).status, 200);
    await destination.close();
    destination = await f.start(f.target, {standby: true});
    const state = (await f.call(destination.control, '/status')).body;
    assert.equal(state.mode, 'paused');
    assert.equal(state.admission, 'open');
    await f.call(destination.control, '/resume', {expected_epoch: state.epoch});
    await until(() => Promise.resolve(f.rows.has(pending.request_id)));
    assert.equal((await f.call(destination.api, '/api/requests', {
      ...committed, payload: 'different'})).status, 409);
    await source.close();
    source = await f.start(f.source, {forwardTargets: {
      aws_receipt: {url: destination.url}}});
    assert.equal((await f.call(source.api, '/api/requests', request(4))).status, 503);
    assert.equal((await f.call(source.control, '/handoff/forward', {
      id: 'migration-one', target: 'aws_receipt'})).status, 200);
    await source.close();
    source = await f.start(f.source, {forwardTargets: {
      aws_receipt: {url: destination.url}}});
    assert.equal((await f.call(source.control, '/resume', {
      expected_epoch: '0'})).status, 409);
    const fresh = request(5);
    assert.equal((await f.call(source.api, '/api/requests', fresh)).status, 202);
    await until(() => Promise.resolve(f.rows.has(fresh.request_id)));
    const ownIdentity = (await f.call(source.api, '/api/receipt')).body;
    assert.notEqual(ownIdentity.ledger_id, destIdentity.ledger_id);
    assert.equal(ownIdentity.admission, 'forwarding');
    await source.close();
    const independent = request(6);
    assert.equal((await f.call(destination.api, '/api/requests', independent)).status, 202);
    await until(() => Promise.resolve(f.rows.has(independent.request_id)));
    assert.equal(f.rows.size, 4);
  });

test('handoff rejects unauthenticated controls, wrong destination, corrupt or incomplete imports',
  {skip: !enabled}, async (t) => {
    const f = await fixture(t);
    const destination = await f.start(f.target, {standby: true});
    const source = await f.start(f.source);
    assert.equal((await f.call(source.control, '/handoff/status', undefined, '')).status, 401);
    assert.equal((await f.call(destination.api, '/api/requests', request(1))).status, 503);
    await f.call(source.api, '/api/requests', request(2));
    const identity = (await f.call(destination.control, '/handoff/status')).body;
    const seal = (await f.call(source.control, '/handoff/seal', {
      id: 'migration-two', destination_ledger_id: identity.ledger_id})).body;
    assert.equal((await f.call(destination.control, '/handoff/activate', {
      ...seal, target: 'aws_eks'})).status, 409);
    const page = (await f.call(source.control,
        '/handoff/export?id=migration-two&cursor=0-0')).body;
    assert.equal((await f.call(destination.control, '/handoff/import', {
      ...page, destination_ledger_id: randomUUID()})).status, 409);
    const corrupt = structuredClone(page);
    corrupt.records[0].body = JSON.stringify(request(9));
    assert.equal((await f.call(destination.control, '/handoff/import', corrupt)).status, 409);
    assert.equal((await f.call(destination.control, '/handoff/import', page)).status, 200);
    assert.equal((await f.call(destination.control, '/handoff/activate', {
      ...seal, target: 'aws_eks'})).status, 200);
    assert.equal((await f.call(destination.control, '/handoff/import', page)).status, 409);
    assert.equal((await f.call(source.control, '/handoff/seal', {
      id: 'different', destination_ledger_id: identity.ledger_id})).status, 409);
  });

test('page replay recovers partial Redis writes and activation rechecks stored content',
  {skip: !enabled}, async (t) => {
    const f = await fixture(t);
    const destination = await f.start(f.target, {standby: true});
    const source = await f.start(f.source);
    for (let i = 0; i < 105; i++) {
      assert.equal((await f.call(source.api, '/api/requests', request(i))).status, 202);
    }
    const destinationId = (await f.call(destination.control, '/handoff/status')).body.ledger_id;
    const seal = (await f.call(source.control, '/handoff/seal', {
      id: 'paged', destination_ledger_id: destinationId})).body;
    const first = (await f.call(source.control,
        '/handoff/export?id=paged&cursor=0-0')).body;
    assert.equal(first.records.length, 100);
    assert.equal(first.done, false);
    const realEval = f.target.eval.bind(f.target);
    let failOnce = true;
    f.target.eval = async (script, options) => {
      if (failOnce && script.includes('local records=cjson.decode')) {
        failOnce = false;
        return realEval(`
          local r=cjson.decode(ARGV[5])[1]
          redis.call('HSET','ingress:handoff','binding',ARGV[2],'state','importing')
          redis.call('HSET','ingress:req:'..r.request_id,
            'body',r.body,'status',r.status,'entry_id',r.entry_id)
          redis.call('XADD','ingress:inbox',r.entry_id,'id',r.request_id)
          return redis.error_reply('SIMULATED_RUNTIME_FAILURE')`, options);
      }
      return realEval(script, options);
    };
    assert.equal((await f.call(destination.control, '/handoff/import', first)).status, 503);
    assert.equal((await f.call(destination.control, '/handoff/import', first)).status, 200);
    await source.close();
    const reopened = await f.start(f.source);
    const replay = (await f.call(reopened.control,
        '/handoff/export?id=paged&cursor=0-0')).body;
    assert.deepEqual(replay, first);
    const second = (await f.call(reopened.control,
        '/handoff/export?id=paged&cursor=' + first.next_cursor)).body;
    assert.equal(second.records.length, 5);
    assert.equal(second.done, true);
    assert.equal((await f.call(destination.control, '/handoff/import', second)).status, 200);
    assert.equal(await f.target.xLen('ingress:inbox'), 105);
    const record = second.records[0];
    await f.target.hSet('ingress:req:' + record.request_id, 'status', 'committed');
    assert.equal((await f.call(destination.control, '/handoff/activate', {
      ...seal, target: 'aws_eks'})).status, 409);
    await f.target.hSet('ingress:req:' + record.request_id, 'status', record.status);
    assert.equal((await f.call(destination.control, '/handoff/activate', {
      ...seal, target: 'aws_eks'})).status, 200);
  });

test('seal races reject new admission and transfer every acknowledged request',
  {skip: !enabled}, async (t) => {
    const f = await fixture(t);
    const source = await f.start(f.source);
    const destination = await f.start(f.target, {standby: true});
    const destinationId = (await f.call(destination.control, '/handoff/status')).body.ledger_id;
    const writes = Array.from({length: 50}, (_, i) =>
      f.call(source.api, '/api/requests', request(i)));
    const seal = f.call(source.control, '/handoff/seal', {
      id: 'race', destination_ledger_id: destinationId});
    const responses = await Promise.all(writes);
    const sealed = await seal;
    assert.equal(sealed.status, 200);
    assert(responses.every((response) => [202, 503].includes(response.status)));
    const accepted = responses.filter((response) => response.status === 202);
    assert.equal(sealed.body.manifest.count, accepted.length);
    assert.equal((await f.call(source.api, '/api/requests', request(99))).status, 503);
    assert.equal(await f.source.xLen('ingress:inbox'), accepted.length);
  });

test('deployed HTTP listener requires separate API and control tokens',
  {skip: !enabled}, async (t) => {
    const {spawn} = require('node:child_process');
    const {createServer} = require('node:net');
    const redis = createClient({url: process.env.TEST_HANDOFF_TARGET_REDIS_URL});
    await redis.connect();
    await redis.flushDb();
    const reserve = createServer().listen(0, '127.0.0.1');
    await once(reserve, 'listening');
    const port = reserve.address().port;
    await new Promise((resolve) => reserve.close(resolve));
    const child = spawn(process.execPath, ['src/durable-ingress/server.js'], {
      env: {...process.env, REDIS_URL: process.env.TEST_HANDOFF_TARGET_REDIS_URL,
        INGRESS_API_PORT: String(port), INGRESS_CONTROL_PORT: '0',
        INGRESS_EXPOSE_CONTROL: 'true', INGRESS_STANDBY: 'true',
        INGRESS_INITIAL_TARGET: 'aws_eks',
        INGRESS_TARGETS: JSON.stringify({aws_eks: {url: 'http://127.0.0.1:1'}}),
        INGRESS_API_TOKEN: 'test-api', INGRESS_CONTROL_TOKEN: 'test-control'},
      stdio: 'ignore',
    });
    t.after(async () => {
      child.kill('SIGTERM');
      await once(child, 'exit');
      await redis.quit();
    });
    const get = (path, token) => fetch(`http://127.0.0.1:${port}${path}`, {
      headers: token ? {Authorization: `Bearer ${token}`} : {},
    });
    await until(async () => {
      try { return (await get('/healthz')).ok; } catch { return false; }
    });
    assert.equal((await get('/api/receipt')).status, 401);
    assert.equal((await get('/api/receipt', 'test-api')).status, 200);
    assert.equal((await get('/control/handoff/status', 'test-api')).status, 401);
    assert.equal((await get('/control/handoff/status', 'test-control')).status, 200);
    assert.equal((await get('/control/status')).status, 401);
  });

test('escaped payloads stay within import body bounds without dropping records',
  {skip: !enabled}, async (t) => {
    const f = await fixture(t);
    const destination = await f.start(f.target, {standby: true});
    const source = await f.start(f.source);
    for (let i = 0; i < 12; i++) {
      assert.equal((await f.call(source.api, '/api/requests',
          request(i, '\"\\\n'.repeat(10000)))).status, 202);
    }
    const destinationId = (await f.call(destination.control, '/handoff/status')).body.ledger_id;
    await f.call(source.control, '/handoff/seal', {
      id: 'escaped', destination_ledger_id: destinationId});
    let cursor = '0-0';
    let count = 0;
    let pages = 0;
    while (true) {
      const page = (await f.call(source.control,
          '/handoff/export?id=escaped&cursor=' + cursor)).body;
      assert(Buffer.byteLength(JSON.stringify(page)) < 1024 * 1024);
      assert.equal((await f.call(destination.control, '/handoff/import', page)).status, 200);
      count += page.records.length;
      pages++;
      if (page.done) break;
      cursor = page.next_cursor;
    }
    assert.equal(count, 12);
    assert(pages > 1);
  });


test('public or standby listeners refuse startup without an API token', async () => {
  const {spawnSync} = require('node:child_process');
  for (const configured of ['INGRESS_EXPOSE_CONTROL', 'INGRESS_STANDBY']) {
    const env = {...process.env, INGRESS_API_TOKEN: '',
      INGRESS_EXPOSE_CONTROL: 'false', INGRESS_STANDBY: 'false',
      INGRESS_TARGETS: '{}', INGRESS_FORWARD_TARGETS: '{}',
      INGRESS_CONTROL_TOKEN: 'test-control', [configured]: 'true'};
    const result = spawnSync(process.execPath,
        ['src/durable-ingress/server.js'], {env, encoding: 'utf8', timeout: 5000});
    assert.equal(result.status, 1);
    assert.match(result.stderr, /INGRESS_START_FAILED/);
    assert(!result.stderr.includes('test-control'));
  }
});
