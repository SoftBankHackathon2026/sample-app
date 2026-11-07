const {test} = require('node:test');
const assert = require('node:assert/strict');
const {once} = require('node:events');
const express = require('express');
const {controlAuth} = require('../src/durable-ingress/service');

async function serve(token) {
  const app = express(); app.use(controlAuth(token)); app.use(express.json());
  app.get('/status', (req, res) => res.json({mode: 'paused'}));
  const srv = app.listen(0); await once(srv, 'listening');
  const call = (auth) => fetch(`http://127.0.0.1:${srv.address().port}/status`,
    {headers: auth ? {Authorization: auth} : {}});
  return {srv, call};
}

test('control API without a token accepts unauthenticated callers (network policy only)', async (t) => {
  const {srv, call} = await serve(undefined); t.after(() => srv.close());
  assert.equal((await call()).status, 200);
});

test('control API with INGRESS_CONTROL_TOKEN requires the exact bearer token', async (t) => {
  const {srv, call} = await serve('s3cret-token'); t.after(() => srv.close());
  assert.equal((await call()).status, 401);
  assert.equal((await call('Bearer wrong')).status, 401);
  assert.equal((await call('Bearer s3cret-token-longer')).status, 401);
  const ok = await call('Bearer s3cret-token');
  assert.equal(ok.status, 200);
  assert.deepEqual(await ok.json(), {mode: 'paused'});
  assert.deepEqual(await (await call()).json(), {error: 'CONTROL_UNAUTHORIZED'});
});
