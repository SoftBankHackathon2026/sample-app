const express = require('express');
const {randomUUID, timingSafeEqual} = require('node:crypto');
const {normalizeRequest, normalizeId} = require('../../packages/request-store/request');
const {createHandoff} = require('./handoff');

// Optional bearer token for the control API. Empty token = rely on network policy only.
function controlAuth(token) {
  if (!token) return (req, res, next) => next();
  const expected = Buffer.from(`Bearer ${token}`);
  return (req, res, next) => {
    const given = Buffer.from(String(req.headers.authorization || ''));
    const ok = given.length === expected.length && timingSafeEqual(given, expected);
    if (!ok) return res.status(401).json({error: 'CONTROL_UNAUTHORIZED'});
    next();
  };
}

// One consumer owns this queue. The lease is also checked by the atomic ACK.
const ENQUEUE = `
if redis.call('GET','ingress:owner')~=ARGV[4] then return 'sealed' end
if redis.call('HGET','ingress:control','admission')~='open' then return 'sealed' end
local old=redis.call('HGET',KEYS[1],'body')
if old then
 if old~=ARGV[1] then return 'conflict' end
 return redis.call('HGET',KEYS[1],'status')
end
if redis.call('XLEN',KEYS[2])>=tonumber(ARGV[3]) then return 'full' end
redis.call('HSET',KEYS[1],'body',ARGV[1],'status','accepted')
local q=redis.pcall('XADD',KEYS[2],'*','id',ARGV[2])
if type(q)=='table' and q.err then
 redis.call('DEL',KEYS[1]);return redis.error_reply('ENQUEUE_FAILED')
end
return 'accepted'`;
const ACK = `
if redis.call('GET','ingress:owner')~=ARGV[1] then return 0 end
redis.call('HSET',KEYS[1],'status','committed')
redis.call('XACK','ingress:inbox','delivery',ARGV[2])
return 1`;

function problem(code, statusCode = 409) {
  return Object.assign(new Error(code), {code, statusCode});
}

async function createIngress({redis, targets, initialTarget = 'onprem',
  maxRecords = 100000, afterCommit, controlToken, standby = false,
  forwardTargets = {}}) {
  if (!targets[initialTarget]) throw new Error('initial target missing');
  for (const target of Object.values(targets)) {
    const u = new URL(target.url);
    if (!['http:', 'https:'].includes(u.protocol) || u.username || u.password ||
        u.search || u.hash || (target.token && u.protocol !== 'https:')) {
      throw new Error('unsafe target URL/token transport');
    }
  }
  const owner = randomUUID();
  if (!await redis.set('ingress:owner', owner, {NX: true, PX: 30000})) {
    throw new Error('CONSUMER_ALREADY_RUNNING');
  }
  let mode = 'paused';
  let active = null;
  let closed = false;
  let leased = true;
  let lastError = null;
  let renewal;
  try {
    await redis.hSetNX('ingress:control', 'target', initialTarget);
    await redis.hSetNX('ingress:control', 'epoch', '0');
    // Every process restart requires an explicit resume; never reset target.
    await redis.hSet('ingress:control', 'mode', 'paused');
    try { await redis.xGroupCreate('ingress:inbox', 'delivery', '0', {MKSTREAM: true}); }
    catch (e) { if (!e.message.includes('BUSYGROUP')) throw e; }
  } catch (e) {
    await redis.eval("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end", {
      keys: ['ingress:owner'], arguments: [owner],
    });
    throw e;
  }
  const renewalTimer = setInterval(() => {
    if (renewal) return;
    renewal = redis.eval("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],30000) else return 0 end", {
      keys: ['ingress:owner'], arguments: [owner],
    }).then((ok) => {
      if (!ok) { leased = false; mode = 'paused'; }
    }).catch(() => { leased = false; mode = 'paused'; })
        .finally(() => { renewal = null; });
  }, 3000);
  renewalTimer.unref();

  async function assertOwner() {
    if (!leased || closed || await redis.get('ingress:owner') !== owner) {
      leased = false; mode = 'paused'; throw problem('LEASE_LOST', 503);
    }
  }
  async function pauseDelivery() {
    mode = 'paused';
    await redis.hSet('ingress:control', 'mode', 'paused');
    if (active) await active;
  }
  let handoff;
  try {
    handoff = await createHandoff({redis, owner, standby, controlToken,
      forwardTargets, pause: pauseDelivery, assertOwner, maxRecords});
  } catch (error) {
    clearInterval(renewalTimer);
    await redis.eval("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end", {
      keys: ['ingress:owner'], arguments: [owner],
    });
    throw error;
  }
  async function controlState() {
    await assertOwner();
    return {...await redis.hGetAll('ingress:control'),
      ...await handoff.status(),
      mode, in_flight: active ? 1 : 0, last_error: lastError};
  }
  async function invoke(method, id, body) {
    const targetName = await redis.hGet('ingress:control', 'target');
    const target = targets[targetName];
    if (!target) throw problem('TARGET_NOT_CONFIGURED', 503);
    const response = await fetch(target.url.replace(/\/$/, '') +
      '/api/requests' + (id ? '/' + encodeURIComponent(id) : ''), {
      method, redirect: 'error', signal: AbortSignal.timeout(5000),
      headers: {'Content-Type': 'application/json',
        ...(target.token ? {Authorization: `Bearer ${target.token}`} : {})},
      ...(body ? {body} : {}),
    });
    if (response.status === 404 && method === 'GET') return null;
    if (!response.ok) throw problem('APP_HTTP_' + response.status, 503);
    return response.json();
  }
  async function deliver() {
    await assertOwner();
    if (mode !== 'running') return;
    let rows = await redis.sendCommand(['XREADGROUP', 'GROUP', 'delivery', 'single',
      'COUNT', '1', 'STREAMS', 'ingress:inbox', '0']);
    if (!rows?.[0]?.[1]?.length) rows = await redis.sendCommand([
      'XREADGROUP', 'GROUP', 'delivery', 'single', 'COUNT', '1',
      'STREAMS', 'ingress:inbox', '>']);
    if (!rows?.[0]?.[1]?.length) return;
    const [entry, fields] = rows[0][1][0];
    const id = fields[1];
    if (await redis.hGet('ingress:req:' + id, 'status') === 'committed') {
      await redis.xAck('ingress:inbox', 'delivery', entry);
      return;
    }
    const body = await redis.hGet('ingress:req:' + id, 'body');
    const posted = await invoke('POST', null, body);
    if (posted.status !== 'committed') throw problem('COMMIT_UNCONFIRMED', 503);
    if (afterCommit) await afterCommit();
    const found = await invoke('GET', id);
    if (!found || found.status !== 'committed' ||
        JSON.stringify(normalizeRequest(found.request)) !== body) {
      throw problem('CONTENT_CONFLICT', 503);
    }
    if (!await redis.eval(ACK, {keys: ['ingress:req:' + id], arguments: [owner, entry]})) {
      throw problem('LEASE_LOST', 503);
    }
    lastError = null;
  }
  function tick() {
    if (closed || mode !== 'running' || active) return active;
    active = deliver().catch(async (e) => {
      lastError = ['CONTENT_CONFLICT', 'APP_HTTP_409', 'LEASE_LOST'].includes(e.code) ?
        e.code : 'DELIVERY_RETRY';
      if (lastError !== 'DELIVERY_RETRY') {
        mode = 'paused'; await redis.hSet('ingress:control', 'mode', 'paused');
      }
    }).finally(() => { active = null; });
    return active;
  }
  const deliveryTimer = setInterval(() => { tick()?.catch(() => {}); }, 20);
  deliveryTimer.unref();
  const api = express(); api.disable('x-powered-by');
  api.use(express.json({limit: '128kb'}));
  const safe = (fn) => async (req, res, next) => {
    try { await fn(req, res, next); }
    catch (e) { res.status(e.statusCode || 503).json({error: {code:
      ['INVALID_REQUEST', 'ID_CONFLICT', 'QUEUE_FULL', 'PAUSE_REQUIRED',
        'EPOCH_CONFLICT', 'TARGET_NOT_CONFIGURED', 'LEASE_LOST',
        'HANDOFF_CONFLICT', 'RECEIPT_SEALED'].includes(e.code) ?
        e.code : 'INGRESS_UNAVAILABLE'}}); }
  };
  api.get('/healthz', (req, res) => res.json({status: 'ok'}));
  api.get('/readyz', safe(async (req, res) => {
    await assertOwner(); await redis.ping(); res.json({status: 'ok'});
  }));
  api.get('/api/receipt', safe(async (req, res) => {
    const state = await handoff.status();
    res.json({ledger_id: state.ledger_id, admission: state.admission,
      handoff_id: state.handoff_id, source_ledger_id: state.source_ledger_id});
  }));
  api.use('/api/requests', safe(async (req, res, next) => {
    await assertOwner();
    const admission = await redis.hGet('ingress:control', 'admission');
    if (admission === 'forwarding') {
      // Express strips the mount path; preserve only supported receipt routes.
      if (!['GET', 'POST'].includes(req.method)) return res.sendStatus(405);
      if (req.method === 'POST' && req.path !== '/') {
        return res.sendStatus(404);
      }
      const suffix = req.method === 'GET' ?
        '/' + normalizeId(decodeURIComponent(req.path.slice(1))) : '';
      const path = '/api/requests' + suffix;
      return handoff.proxy({method: req.method, path, body: req.body}, res);
    }
    if (admission === 'standby') throw problem('RECEIPT_SEALED', 503);
    if (admission === 'sealed' && req.method !== 'GET') {
      throw problem('RECEIPT_SEALED', 503);
    }
    next();
  }));
  api.post('/api/requests', safe(async (req, res) => {
    const request = normalizeRequest(req.body);
    const state = await redis.eval(ENQUEUE, {
      keys: ['ingress:req:' + request.request_id, 'ingress:inbox'],
      arguments: [JSON.stringify(request), request.request_id, String(maxRecords), owner],
    });
    if (state === 'sealed') throw problem('RECEIPT_SEALED', 503);
    if (state === 'conflict') throw problem('ID_CONFLICT');
    if (state === 'full') throw problem('QUEUE_FULL', 503);
    // Even a duplicate committed ID is read through the current application.
    const found = state === 'committed' ? await invoke('GET', request.request_id) : null;
    if (found?.status === 'committed') {
      if (JSON.stringify(normalizeRequest(found.request)) !== JSON.stringify(request)) {
        throw problem('CONTENT_CONFLICT', 503);
      }
      return res.json(found);
    }
    res.status(202).json({status: 'accepted', request});
  }));
  api.get('/api/requests/:id', safe(async (req, res) => {
    const id = normalizeId(req.params.id);
    const row = await redis.hGetAll('ingress:req:' + id);
    if (!row.body) return res.status(404).json({error: {code: 'REQUEST_NOT_FOUND'}});
    if (row.status === 'committed') {
      const found = await invoke('GET', id);
      if (!found || JSON.stringify(normalizeRequest(found.request)) !== row.body) {
        throw problem('CONTENT_CONFLICT', 503);
      }
      return res.json(found);
    }
    res.status(202).json({status: 'accepted', request: JSON.parse(row.body)});
  }));
  const control = express();
  control.use(controlAuth(controlToken));
  control.use(express.json({limit: '1mb'}));
  // Server binds this app to loopback only. No Service exposes its port.
  let changing = false;
  const exclusive = (fn) => safe(async (req, res) => {
    if (changing) throw problem('CONTROL_BUSY');
    changing = true;
    try { await assertOwner(); await fn(req, res); }
    finally { changing = false; }
  });
  control.get('/status', safe(async (req, res) => res.json(await controlState())));
  control.post('/pause', exclusive(async (req, res) => {
    await pauseDelivery();
    res.json({...await controlState(), settled: true});
  }));
  control.post('/target', exclusive(async (req, res) => {
    if (await redis.hGet('ingress:control', 'admission') !== 'open') throw problem('HANDOFF_CONFLICT');
    if (mode !== 'paused' || active) throw problem('PAUSE_REQUIRED');
    if (!Object.hasOwn(targets, req.body.target)) throw problem('TARGET_NOT_CONFIGURED');
    const epoch = await redis.hGet('ingress:control', 'epoch');
    if (req.body.expected_epoch !== epoch) throw problem('EPOCH_CONFLICT');
    // Operator changes target only AFTER CDC equality and DB fencing succeed.
    await redis.hSet('ingress:control', {
      target: req.body.target, epoch: String(Number(epoch) + 1),
    });
    res.json(await controlState());
  }));
  control.post('/resume', exclusive(async (req, res) => {
    if (await redis.hGet('ingress:control', 'admission') !== 'open') throw problem('HANDOFF_CONFLICT');
    if (req.body.expected_epoch !== await redis.hGet('ingress:control', 'epoch')) {
      throw problem('EPOCH_CONFLICT');
    }
    await redis.hSet('ingress:control', 'mode', 'running'); mode = 'running';
    res.json(await controlState());
  }));
  control.use('/handoff', (req, res, next) => {
    if (!controlToken) return res.status(503).json({error: {code: 'HANDOFF_AUTH_REQUIRED'}});
    next();
  });
  control.get('/handoff/status', safe(async (req, res) => {
    res.json(await handoff.status());
  }));
  control.post('/handoff/seal', exclusive(async (req, res) => {
    res.json(await handoff.seal(req.body));
  }));
  control.get('/handoff/export', exclusive(async (req, res) => {
    res.json(await handoff.exportPage(req.query.id, req.query.cursor || '0-0'));
  }));
  control.post('/handoff/import', exclusive(async (req, res) => {
    res.json(await handoff.importPage(req.body));
  }));
  control.post('/handoff/activate', exclusive(async (req, res) => {
    if (!Object.hasOwn(targets, req.body.target)) throw problem('TARGET_NOT_CONFIGURED');
    res.json(await handoff.activate(req.body));
  }));
  control.post('/handoff/forward', exclusive(async (req, res) => {
    res.json(await handoff.forward(req.body));
  }));
  return {api, control, async close() {
    mode = 'paused'; closed = true; clearInterval(deliveryTimer);
    clearInterval(renewalTimer); if (renewal) await renewal;
    if (active) await active;
    await redis.eval("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end", {
      keys: ['ingress:owner'], arguments: [owner],
    });
  }};
}
module.exports = {createIngress, controlAuth, HANDOFF_PROTOCOL_VERSION: 1};
