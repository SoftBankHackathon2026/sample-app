const {test} = require('node:test');
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {once} = require('node:events');
const express = require('express');
const {createClient} = require('redis');
const {createIngress} = require('../src/durable-ingress/service');

async function eventually(fn) {
  const deadline = Date.now() + 5000;
  while (!await fn()) {
    assert.ok(Date.now() < deadline, 'condition did not settle');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test('ten concurrent deliveries drain together; lost ACK retries only the failed ID after restart',
  {skip: !process.env.TEST_REDIS_URL, timeout: 15000}, async (t) => {
    const r = createClient({url: process.env.TEST_REDIS_URL}); await r.connect(); await r.flushDb();
    const rows = new Map(); const attempts = new Map();
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    let live = 0; let peak = 0;
    const backend = express(); backend.use(express.json());
    backend.post('/api/requests', async (req, res) => {
      live++; peak = Math.max(peak, live);
      const id = req.body.request_id;
      attempts.set(id, (attempts.get(id) || 0) + 1);
      await gate;
      rows.set(id, req.body); live--;
      res.json({status: 'committed'});
    });
    backend.get('/api/requests/:id', (req, res) => res.json({
      status: 'committed', request: rows.get(req.params.id),
    }));
    const host = backend.listen(0); await once(host, 'listening');
    const targets = {onprem: {url: `http://127.0.0.1:${host.address().port}`}};
    let failed = false;
    let app = await createIngress({redis: r, targets, afterCommit: async () => {
      if (!failed) { failed = true; throw new Error('lost ACK'); }
    }});
    let api = app.api.listen(0); let ctl = app.control.listen(0); await once(ctl, 'listening');
    t.after(async () => { release(); await app.close(); api.close(); ctl.close(); host.close(); await r.quit(); });
    const call = async (server, path, body) => {
      const res = await fetch(`http://127.0.0.1:${server.address().port}${path}`, {
        method: body ? 'POST' : 'GET', headers: {'Content-Type': 'application/json'},
        ...(body ? {body: JSON.stringify(body)} : {}),
      });
      assert.ok(res.ok, `${path}: ${res.status}`); return res.json();
    };
    for (let n = 1; n <= 25; n++) {
      await call(api, '/api/requests', {request_id: randomUUID(), run_id: 'batch', phase: 'test',
        request_no: String(n), payload: 'data', created_at: new Date().toISOString()});
    }
    await call(ctl, '/resume', {expected_epoch: '0'});
    await eventually(() => live === 10);
    assert.equal((await call(ctl, '/status')).in_flight, 10);
    let paused = false;
    const pause = call(ctl, '/pause', {}).then((v) => { paused = true; return v; });
    await eventually(async () => (await call(ctl, '/status')).mode === 'paused');
    assert.equal(paused, false);
    release();
    assert.equal((await pause).in_flight, 0);
    assert.equal(rows.size, 10, 'pause must not start the next batch');
    assert.equal((await r.xPending('ingress:inbox', 'delivery')).pending, 1);
    assert.equal((await call(ctl, '/status')).last_error, 'DELIVERY_RETRY');
    await app.close(); api.close(); ctl.close();
    app = await createIngress({redis: r, targets});
    api = app.api.listen(0); ctl = app.control.listen(0); await once(ctl, 'listening');
    assert.equal((await call(ctl, '/status')).mode, 'paused');
    await call(ctl, '/resume', {expected_epoch: '0'});
    await eventually(async () => rows.size === 25 && (await r.xPending('ingress:inbox', 'delivery')).pending === 0);
    await call(ctl, '/pause', {});
    assert.equal(peak, 10);
    assert.equal([...attempts.values()].reduce((a, b) => a + b, 0), 26);
    for (const id of rows.keys()) assert.equal(await r.hGet('ingress:req:' + id, 'status'), 'committed');
  });

test('invalid concurrency fails before acquiring the owner lease', async () => {
  for (const concurrency of [0, 11, 1.5, NaN]) {
    await assert.rejects(createIngress({redis: {}, targets: {}, concurrency}), /concurrency/);
  }
});

test('a conflict pauses the batch after other deliveries settle and never starts the next batch',
  {skip: !process.env.TEST_REDIS_URL, timeout: 10000}, async (t) => {
    const r = createClient({url: process.env.TEST_REDIS_URL}); await r.connect(); await r.flushDb();
    const rows = new Map(); let posts = 0;
    const backend = express(); backend.use(express.json());
    backend.post('/api/requests', (req, res) => {
      posts++;
      if (req.body.request_no === '1') return res.sendStatus(409);
      rows.set(req.body.request_id, req.body); res.json({status: 'committed'});
    });
    backend.get('/api/requests/:id', (req, res) => res.json({status: 'committed', request: rows.get(req.params.id)}));
    const host = backend.listen(0); await once(host, 'listening');
    const app = await createIngress({redis: r, targets: {onprem: {url: `http://127.0.0.1:${host.address().port}`}}});
    const api = app.api.listen(0); const ctl = app.control.listen(0); await once(ctl, 'listening');
    t.after(async () => { await app.close(); api.close(); ctl.close(); host.close(); await r.quit(); });
    for (let n = 1; n <= 20; n++) {
      const response = await fetch(`http://127.0.0.1:${api.address().port}/api/requests`, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({request_id: randomUUID(), run_id: 'conflict', phase: 'test',
          request_no: String(n), payload: 'data', created_at: new Date().toISOString()}),
      });
      assert.equal(response.status, 202); await response.json();
    }
    await fetch(`http://127.0.0.1:${ctl.address().port}/resume`, {method: 'POST',
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify({expected_epoch: '0'})});
    const status = async () => (await fetch(`http://127.0.0.1:${ctl.address().port}/status`)).json();
    await eventually(async () => (await status()).last_error === 'APP_HTTP_409');
    assert.equal((await status()).mode, 'paused');
    assert.equal((await status()).in_flight, 0);
    assert.equal(posts, 10); assert.equal(rows.size, 9);
    assert.equal((await r.xPending('ingress:inbox', 'delivery')).pending, 1);
  });

test('live control gates writes, drains in-flight, fences stale epoch and preserves accepted data',
  {skip: !process.env.TEST_REDIS_URL}, async (t) => {
    const r = createClient({url: process.env.TEST_REDIS_URL});
    await r.connect();
    // TEST_REDIS_URL must name a dedicated disposable Redis instance.
    await r.flushDb();
    const rows = new Map(); let release; let started;
    let wait = new Promise((resolve) => { release = resolve; });
    const writing = new Promise((resolve) => { started = resolve; });
    const backend = express(); backend.use(express.json());
    backend.post('/api/requests', async (req, res) => {
      started(); await wait; rows.set(req.body.request_id, req.body);
      res.json({status: 'committed'});
    });
    backend.get('/api/requests/:id', (req, res) => {
      const request = rows.get(req.params.id);
      if (!request) return res.sendStatus(404);
      res.json({status: 'committed', request, database: {name: 'test'}, environment: 'test'});
    });
    const host = backend.listen(0); await once(host, 'listening');
    const targets = {onprem: {url: `http://127.0.0.1:${host.address().port}`},
      aws: {url: `http://127.0.0.1:${host.address().port}`}};
    let app = await createIngress({redis: r, targets});
    let api = app.api.listen(0); let ctl = app.control.listen(0); await once(ctl, 'listening');
    t.after(async () => { release(); await app.close(); api.close(); ctl.close(); host.close(); await r.quit(); });
    const call = async (server, path, body) => {
      const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, {
        method: body ? 'POST' : 'GET', headers: {'Content-Type': 'application/json'},
        ...(body ? {body: JSON.stringify(body)} : {}),
      });
      return {status: response.status, body: await response.json()};
    };
    const request = {request_id: randomUUID(), run_id: 'control', phase: 'test',
      request_no: '1', payload: 'data', created_at: new Date().toISOString()};
    assert.equal((await call(api, '/api/requests', request)).status, 202);
    assert.equal(rows.size, 0);
    await assert.rejects(createIngress({redis: r, targets}), /CONSUMER_ALREADY_RUNNING/);
    assert.equal((await call(ctl, '/resume', {expected_epoch: '9'})).status, 409);
    await call(ctl, '/resume', {expected_epoch: '0'}); await writing;
    let paused = false;
    const pause = call(ctl, '/pause', {}).then((result) => { paused = true; return result; });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(paused, false, 'pause must wait for active DB commit/readback');
    release(); const result = await pause;
    assert.equal(result.body.settled, true); assert.equal(result.body.in_flight, 0);
    assert.equal((await call(api, '/api/requests/' + request.request_id)).body.status, 'committed');
    assert.equal((await call(ctl, '/target', {target: 'aws', expected_epoch: '0'})).body.epoch, '1');
    assert.equal((await call(ctl, '/resume', {expected_epoch: '0'})).status, 409);
    await app.close(); api.close(); ctl.close();
    app = await createIngress({redis: r, targets});
    api = app.api.listen(0); ctl = app.control.listen(0); await once(ctl, 'listening');
    const state = (await call(ctl, '/status')).body;
    assert.equal(state.target, 'aws'); assert.equal(state.epoch, '1'); assert.equal(state.mode, 'paused');
    assert.equal((await call(api, '/api/requests', {...request, payload: 'different'})).status, 409);
    assert.equal(rows.size, 1);
  });
