const {createHash, randomUUID} = require('node:crypto');
const {normalizeRequest} = require('../../packages/request-store/request');

const EMPTY_DIGEST = createHash('sha256').update('').digest('hex');
const PAGE_RECORDS = 100;
const PAGE_BYTES = 768 * 1024;
const NAME = /^[a-zA-Z0-9_-]{1,128}$/;
const CURSOR = /^\d+-\d+$/;
const DIGEST = /^[a-f0-9]{64}$/;
const HANDOFF = 'ingress:handoff';
const CONTROL = 'ingress:control';

function validName(value) {
  return typeof value === 'string' && NAME.test(value);
}

function conflict() {
  return Object.assign(new Error('HANDOFF_CONFLICT'), {
    code: 'HANDOFF_CONFLICT', statusCode: 409,
  });
}

function recordDigest(previous, record) {
  return createHash('sha256').update(previous + '\n' + JSON.stringify([
    record.entry_id, record.request_id, record.body, record.status,
  ])).digest('hex');
}

function validManifest(manifest) {
  return manifest && Number.isSafeInteger(manifest.count) &&
    manifest.count >= 0 && DIGEST.test(manifest.digest) &&
    CURSOR.test(manifest.last_cursor);
}

function sameManifest(a, b) {
  return a.count === b.count && a.digest === b.digest &&
    a.last_cursor === b.last_cursor;
}

function later(a, b) {
  const left = a.split('-').map(BigInt);
  const right = b.split('-').map(BigInt);
  return left[0] > right[0] ||
    (left[0] === right[0] && left[1] > right[1]);
}

// Each record write is replayable after a Redis runtime error: scripts are
// isolated, but Redis does not roll back writes made before a runtime error.
const IMPORT_PAGE = `
if redis.call('GET','ingress:owner')~=ARGV[1] then return 'owner' end
if redis.call('HGET','ingress:control','admission')~='standby' then return 'state' end
local old=redis.call('HGET','ingress:handoff','binding')
if old and old~=ARGV[2] then return 'binding' end
local page=redis.call('HGET','ingress:handoff','page:'..ARGV[3])
if page then
 if page==ARGV[4] then return 'replayed' else return 'page' end
end
local cursor=redis.call('HGET','ingress:handoff','cursor') or '0-0'
if cursor~=ARGV[3] then return 'cursor' end
if not old and redis.call('XLEN','ingress:inbox')~=0 then return 'nonempty' end
local records=cjson.decode(ARGV[5])
for _,r in ipairs(records) do
 local key='ingress:req:'..r.request_id
 local body=redis.call('HGET',key,'body')
 local status=redis.call('HGET',key,'status')
 if body and (body~=r.body or status~=r.status) then return 'record' end
 local rows=redis.call('XRANGE','ingress:inbox',r.entry_id,r.entry_id)
 if #rows>0 and rows[1][2][2]~=r.request_id then return 'entry' end
end
redis.call('HSET','ingress:handoff','binding',ARGV[2],'state','importing')
for _,r in ipairs(records) do
 redis.call('HSET','ingress:req:'..r.request_id,'body',r.body,
  'status',r.status,'entry_id',r.entry_id)
 if #redis.call('XRANGE','ingress:inbox',r.entry_id,r.entry_id)==0 then
  redis.call('XADD','ingress:inbox',r.entry_id,'id',r.request_id)
 end
end
redis.call('HSET','ingress:handoff','cursor',ARGV[6],
 'imported_count',ARGV[7],'imported_digest',ARGV[8],
 'page:'..ARGV[3],ARGV[4])
return 'ok'`;

async function createHandoff({redis, owner, standby, controlToken,
  forwardTargets, pause, assertOwner, maxRecords}) {
  if (standby && !controlToken) throw new Error('HANDOFF_AUTH_REQUIRED');
  await redis.set('ingress:ledger', randomUUID(), {NX: true});
  await redis.hSetNX(CONTROL, 'admission', standby ? 'standby' : 'open');
  const ledgerId = await redis.get('ingress:ledger');
  if (standby && await redis.hGet(CONTROL, 'admission') === 'open' &&
      await redis.hGet(CONTROL, 'handoff_state') !== 'active') {
    throw new Error('STANDBY_LEDGER_NOT_EMPTY');
  }
  for (const target of Object.values(forwardTargets)) {
    const url = new URL(target.url);
    if (!['http:', 'https:'].includes(url.protocol) || url.username ||
        url.password || url.search || url.hash ||
        (target.token && url.protocol !== 'https:')) {
      throw new Error('UNSAFE_FORWARD_TARGET');
    }
  }

  async function status() {
    await assertOwner();
    const fields = ['binding', 'state', 'cursor', 'imported_count',
      'imported_digest'];
    const values = await redis.hmGet(HANDOFF, fields);
    const data = Object.fromEntries(fields.map((field, index) =>
      [field, values[index]]));
    const control = await redis.hGetAll(CONTROL);
    const raw = control.handoff_binding || data.binding;
    const binding = raw ? JSON.parse(raw) : {};
    return {ledger_id: ledgerId,
      admission: control.admission,
      handoff_id: binding.id || null, id: binding.id || null,
      state: control.handoff_state || data.state || 'none',
      source_ledger_id: binding.source_ledger_id || null,
      destination_ledger_id: binding.destination_ledger_id || null,
      manifest: binding.manifest || null,
      cursor: data.cursor || '0-0',
      imported_count: Number(data.imported_count || 0),
      imported_digest: data.imported_digest || EMPTY_DIGEST};
  }

  async function recordsAfter(cursor) {
    const entries = await redis.xRange('ingress:inbox', '(' + cursor, '+', {
      COUNT: PAGE_RECORDS,
    });
    const records = [];
    let bytes = 2;
    for (const entry of entries) {
      const row = await redis.hGetAll('ingress:req:' + entry.message.id);
      if (!row.body || !['accepted', 'committed'].includes(row.status)) {
        throw conflict();
      }
      const record = {entry_id: entry.id, request_id: entry.message.id,
        body: row.body, status: row.status};
      bytes += Buffer.byteLength(JSON.stringify(record)) +
        (records.length ? 1 : 0);
      if (bytes > PAGE_BYTES && records.length) break;
      if (bytes > PAGE_BYTES) throw conflict();
      records.push(record);
    }
    return records;
  }

  async function manifest() {
    let cursor = '0-0';
    let digest = EMPTY_DIGEST;
    let count = 0;
    while (true) {
      const records = await recordsAfter(cursor);
      if (!records.length) break;
      for (const record of records) digest = recordDigest(digest, record);
      count += records.length;
      cursor = records[records.length - 1].entry_id;
    }
    if (count !== await redis.xLen('ingress:inbox')) throw conflict();
    return {count, digest, last_cursor: cursor};
  }

  async function seal({id, destination_ledger_id: destination}) {
    if (!validName(id) || !validName(destination) || destination === ledgerId) {
      throw conflict();
    }
    const binding = JSON.stringify({id, source_ledger_id: ledgerId,
      destination_ledger_id: destination});
    const result = await redis.eval(`
      if redis.call('GET','ingress:owner')~=ARGV[1] then return 'owner' end
      local old=redis.call('HGET','ingress:control','seal_binding')
      if old then return old==ARGV[2] and 'ok' or 'binding' end
      if redis.call('HGET','ingress:control','admission')~='open' then return 'state' end
      if redis.call('HGET','ingress:handoff','state') then return 'state' end
      redis.call('HSET','ingress:control','admission','sealed','mode','paused',
        'seal_binding',ARGV[2],'handoff_binding',ARGV[2],'handoff_state','sealing')
      return 'ok'`, {arguments: [owner, binding]});
    if (result !== 'ok') throw conflict();
    await pause();
    const current = await status();
    if (current.state === 'forwarding') return current;
    if (!current.manifest) {
      const summary = await manifest();
      const saved = await redis.eval(`
        if redis.call('GET','ingress:owner')~=ARGV[1] then return 0 end
        redis.call('HSET','ingress:control','handoff_binding',ARGV[2],
          'handoff_state','sealed')
        return 1`, {arguments: [owner, JSON.stringify({
        ...JSON.parse(binding), manifest: summary})]});
      if (!saved) throw conflict();
    }
    return status();
  }

  async function exportPage(id, cursor) {
    const current = await status();
    if (!['sealed', 'forwarding'].includes(current.state) ||
        current.handoff_id !== id || !CURSOR.test(cursor)) throw conflict();
    const records = await recordsAfter(cursor);
    const next = records.length ? records[records.length - 1].entry_id : cursor;
    return {id, source_ledger_id: ledgerId,
      destination_ledger_id: current.destination_ledger_id,
      manifest: current.manifest, cursor, next_cursor: next, records,
      done: next === current.manifest.last_cursor};
  }

  async function importPage(page) {
    if (!validName(page.id) || !validName(page.source_ledger_id) ||
        page.source_ledger_id === ledgerId ||
        page.destination_ledger_id !== ledgerId ||
        !validManifest(page.manifest) || page.manifest.count > maxRecords ||
        !CURSOR.test(page.cursor) ||
        !CURSOR.test(page.next_cursor) || !Array.isArray(page.records) ||
        page.records.length > PAGE_RECORDS ||
        Buffer.byteLength(JSON.stringify(page.records)) > PAGE_BYTES) {
      throw conflict();
    }
    const current = await status();
    if (current.admission !== 'standby') throw conflict();
    const binding = JSON.stringify({id: page.id,
      source_ledger_id: page.source_ledger_id,
      destination_ledger_id: ledgerId, manifest: page.manifest});
    const previousBinding = await redis.hGet(HANDOFF, 'binding');
    if (previousBinding && previousBinding !== binding) throw conflict();
    if (!previousBinding) {
      // Standby is not permission to overwrite an orphaned receipt ledger.
      for await (const keys of redis.scanIterator({MATCH: 'ingress:req:*'})) {
        if (keys.length) throw conflict();
      }
    }
    const pageHash = createHash('sha256').update(JSON.stringify(page)).digest('hex');
    const replay = await redis.hGet(HANDOFF, 'page:' + page.cursor);
    if (replay) {
      if (replay !== pageHash) throw conflict();
      return status();
    }
    if (page.cursor !== current.cursor) throw conflict();
    let cursor = page.cursor;
    let digest = current.imported_digest;
    let count = current.imported_count;
    const ids = new Set();
    for (const record of page.records) {
      if (!CURSOR.test(record.entry_id) || !later(record.entry_id, cursor) ||
          typeof record.body !== 'string' ||
          !['accepted', 'committed'].includes(record.status)) throw conflict();
      let request;
      try { request = normalizeRequest(JSON.parse(record.body)); }
      catch { throw conflict(); }
      if (JSON.stringify(request) !== record.body ||
          request.request_id !== record.request_id ||
          ids.has(record.request_id)) throw conflict();
      ids.add(record.request_id);
      digest = recordDigest(digest, record);
      cursor = record.entry_id;
      count++;
    }
    if (cursor !== page.next_cursor || count > page.manifest.count ||
        (page.done !== (cursor === page.manifest.last_cursor)) ||
        (!page.records.length && page.manifest.count !== 0) ||
        (page.done && (count !== page.manifest.count ||
          digest !== page.manifest.digest))) throw conflict();
    const result = await redis.eval(IMPORT_PAGE, {arguments: [owner, binding,
      page.cursor, pageHash, JSON.stringify(page.records), cursor,
      String(count), digest]});
    if (!['ok', 'replayed'].includes(result)) throw conflict();
    return status();
  }

  async function activate(body) {
    const current = await status();
    if (current.handoff_id !== body.id ||
        current.source_ledger_id !== body.source_ledger_id ||
        current.destination_ledger_id !== body.destination_ledger_id ||
        !validManifest(body.manifest) || !current.manifest ||
        !sameManifest(current.manifest, body.manifest)) throw conflict();
    if (current.state === 'active') {
      if (await redis.hGet(CONTROL, 'target') !== body.target) throw conflict();
      return status();
    }
    if (current.admission !== 'standby' ||
        current.cursor !== body.manifest.last_cursor ||
        current.imported_count !== body.manifest.count ||
        current.imported_digest !== body.manifest.digest ||
        !sameManifest(await manifest(), body.manifest)) throw conflict();
    const result = await redis.eval(`
      if redis.call('GET','ingress:owner')~=ARGV[1] then return 0 end
      if redis.call('HGET','ingress:control','admission')~='standby' then return 0 end
      local epoch=tonumber(redis.call('HGET','ingress:control','epoch'))+1
      redis.call('HSET','ingress:control','admission','open','mode','paused',
        'handoff_state','active','target',ARGV[2],'epoch',tostring(epoch))
      return 1`, {arguments: [owner, body.target]});
    if (!result) throw conflict();
    return status();
  }

  async function forward({id, target: name}) {
    const current = await status();
    const target = Object.hasOwn(forwardTargets, name) ?
      forwardTargets[name] : null;
    if (!target || current.handoff_id !== id ||
        !['sealed', 'forwarding'].includes(current.state)) throw conflict();
    const response = await fetch(target.url.replace(/\/$/, '') + '/api/receipt', {
      redirect: 'error', signal: AbortSignal.timeout(5000),
      headers: target.token ? {Authorization: `Bearer ${target.token}`} : {},
    });
    if (!response.ok) throw conflict();
    const receipt = await response.json();
    if (receipt.ledger_id !== current.destination_ledger_id ||
        receipt.handoff_id !== id || receipt.source_ledger_id !== ledgerId ||
        receipt.admission !== 'open') throw conflict();
    const result = await redis.eval(`
      if redis.call('GET','ingress:owner')~=ARGV[1] then return 0 end
      local old=redis.call('HGET','ingress:control','forward_target')
      if old and old~=ARGV[2] then return 0 end
      redis.call('HSET','ingress:control','admission','forwarding','mode','paused',
        'handoff_state','forwarding','forward_target',ARGV[2])
      return 1`, {arguments: [owner, name]});
    if (!result) throw conflict();
    return status();
  }

  async function proxy(req, res) {
    const name = await redis.hGet(CONTROL, 'forward_target');
    const target = Object.hasOwn(forwardTargets, name) ?
      forwardTargets[name] : null;
    if (!target) throw conflict();
    const response = await fetch(target.url.replace(/\/$/, '') + req.path, {
      method: req.method, redirect: 'error', signal: AbortSignal.timeout(5000),
      headers: {'Content-Type': 'application/json',
        ...(target.token ? {Authorization: `Bearer ${target.token}`} : {})},
      ...(req.method === 'POST' ? {body: JSON.stringify(req.body)} : {}),
    });
    const data = await response.json();
    res.set('X-Ingress-Forwarded', 'true').status(response.status).json(data);
  }

  return {status, seal, exportPage, importPage, activate, forward, proxy};
}

module.exports = {createHandoff};
