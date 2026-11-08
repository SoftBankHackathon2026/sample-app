// Demo-only process lifecycle. The management socket is loopback-only and
// requires the existing control token. No Kubernetes resources are mutated.
const fs = require('node:fs/promises');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {createServer} = require('node:http');
const {timingSafeEqual, randomUUID} = require('node:crypto');
const {createClient} = require('redis');

const CYCLE = /^[a-z][a-z0-9_]{0,47}$/;
const PORT = 9093;
function fail(code) {
  throw Object.assign(new Error(code), {code});
}
function config(env) {
  if (env.DEMO_RESET_ENABLED !== 'true') fail('DEMO_RESET_DISABLED');
  const role = env.DEMO_ROLE;
  if (!['bot', 'source', 'destination'].includes(role))
    fail('DEMO_ROLE_REQUIRED');
  const token =
    role === 'bot' ? env.BOT_CONTROL_TOKEN : env.INGRESS_CONTROL_TOKEN;
  if (!token || token.length < 32) fail('DEMO_CONTROL_TOKEN_REQUIRED');
  return {role, token};
}
function authorized(actual, token) {
  const a = Buffer.from(actual || ''),
    b = Buffer.from(`Bearer ${token}`);
  return a.length === b.length && timingSafeEqual(a, b);
}

async function openState(env, role) {
  if (role === 'bot') {
    const root = path.dirname(env.BOT_LEDGER_DIR);
    const file = path.join(root, 'demo-supervisor.json');
    return {
      async read() {
        try {
          return JSON.parse(await fs.readFile(file, 'utf8'));
        } catch (e) {
          if (e.code === 'ENOENT') return {};
          throw e;
        }
      },
      async write(state) {
        const tmp = `${file}.${randomUUID()}.tmp`;
        const h = await fs.open(tmp, 'wx', 0o600);
        try {
          await h.writeFile(JSON.stringify(state));
          await h.sync();
        } finally {
          await h.close();
        }
        await fs.rename(tmp, file);
        const dir = await fs.open(root, 'r');
        try {
          await dir.sync();
        } finally {
          await dir.close();
        }
      },
      async checkCycle(cycle) {
        try {
          const files = await fs.readdir(path.join(root, 'demo-cycles', cycle));
          if (files.length) fail('CYCLE_ALREADY_USED');
        } catch (e) {
          if (e.code !== 'ENOENT') throw e;
        }
      },
      async reset(cycle) {
        const dir = path.join(root, 'demo-cycles', cycle);
        // Reusing an existing directory could replay requests from a prior demo.
        await fs.mkdir(path.dirname(dir), {recursive: true});
        try {
          await fs.mkdir(dir);
        } catch (e) {
          // An interrupted reset may have created an empty directory already.
          if (e.code !== 'EEXIST' || (await fs.readdir(dir)).length) throw e;
        }
      },
      childEnv(cycle) {
        return cycle
          ? {
              BOT_LEDGER_DIR: path.join(root, 'demo-cycles', cycle),
              BOT_RUN_ID: cycle,
              BOT_GENERATION: 'stopped',
            }
          : {};
      },
      async close() {},
    };
  }
  const redis = createClient({url: env.REDIS_URL});
  redis.on('error', () => {});
  await redis.connect();
  return {
    async read() {
      return JSON.parse((await redis.get('demo:supervisor')) || '{}');
    },
    async write(state) {
      await redis.set('demo:supervisor', JSON.stringify(state));
    },
    async checkCycle(cycle) {
      if (await redis.exists(`demo:used:${cycle}`)) fail('CYCLE_ALREADY_USED');
    },
    async reset(cycle) {
      if (await redis.exists('ingress:owner'))
        fail('RECEIPT_OWNER_STILL_ACTIVE');
      if (await redis.exists(`demo:used:${cycle}`)) {
        if ((await redis.keys('ingress:*')).length) fail('CYCLE_ALREADY_USED');
        return; // Reset finished, but pointer persistence was interrupted.
      }
      // Keep the previous ledger for inspection; never FLUSHDB or delete PVCs.
      // Re-run after interruption continues moving only the remaining live keys.
      let cursor = '0';
      do {
        const page = await redis.scan(cursor, {MATCH: 'ingress:*', COUNT: 100});
        cursor = page.cursor;
        for (const key of page.keys) {
          if (!(await redis.exists(key))) continue;
          const moved = await redis.renameNX(
            key,
            `demo:archive:${cycle}:${key}`,
          );
          if (!moved) fail('ARCHIVE_CONFLICT');
        }
      } while (String(cursor) !== '0');
      // SCAN while renaming may miss keys; complete bounded passes until empty.
      const remaining = await redis.keys('ingress:*');
      for (const key of remaining) {
        if (!(await redis.renameNX(key, `demo:archive:${cycle}:${key}`)))
          fail('ARCHIVE_CONFLICT');
      }
      await redis.set(`demo:used:${cycle}`, '1');
    },
    childEnv() {
      return {
        INGRESS_INITIAL_TARGET: role === 'source' ? 'onprem' : 'aws_eks',
        INGRESS_STANDBY: role === 'destination' ? 'true' : 'false',
      };
    },
    async close() {
      await redis.quit();
    },
  };
}

async function serve(env = process.env) {
  const {role, token} = config(env);
  const store = await openState(env, role);
  let state = await store.read(),
    child,
    busy = false,
    closing = false;
  if (
    role !== 'bot' &&
    require('../src/durable-ingress/service').HANDOFF_PROTOCOL_VERSION !== 1
  ) {
    fail('HANDOFF_IMAGE_REQUIRED');
  }
  const childFile =
    role === 'bot'
      ? 'packages/traffic-bot/bin/traffic-bot.js'
      : 'src/durable-ingress/server.js';
  function startChild() {
    if (child || closing) return;
    child = spawn(process.execPath, [childFile], {
      env: {...env, ...store.childEnv(state.cycle)},
      stdio: 'inherit',
    });
    const current = child;
    current.once('exit', () => {
      if (child === current) child = null;
    });
  }
  async function stopChild() {
    if (!child) return;
    const current = child;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('CHILD_STOP_TIMEOUT')),
        25000,
      );
      current.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
      current.kill('SIGTERM');
    });
  }
  async function command(body) {
    if (body.action === 'status') return;
    if (body.action === 'stop') {
      state = {...state, stopped: true};
      await store.write(state);
      await stopChild();
      return;
    }
    if (!CYCLE.test(body.cycle || '')) fail('INVALID_CYCLE');
    if (body.action === 'check-cycle') {
      if (state.resetting !== body.cycle) await store.checkCycle(body.cycle);
      return;
    }
    if (body.action === 'reset') {
      if (!state.stopped || child) fail('STOP_REQUIRED');
      if (state.cycle === body.cycle && !state.resetting) return;
      if (state.resetting && state.resetting !== body.cycle)
        fail('RESET_INCOMPLETE');
      if (!state.resetting) await store.checkCycle(body.cycle);
      state = {...state, resetting: body.cycle};
      await store.write(state);
      await store.reset(body.cycle);
      state = {cycle: body.cycle, stopped: true};
      await store.write(state);
      return;
    }
    if (body.action === 'start') {
      if (state.resetting || state.cycle !== body.cycle) fail('RESET_REQUIRED');
      state = {...state, stopped: false};
      await store.write(state);
      startChild();
      return;
    }
    fail('UNKNOWN_ACTION');
  }
  const server = createServer(async (req, res) => {
    const reply = (status, value) => {
      res.writeHead(status, {'Content-Type': 'application/json'});
      res.end(JSON.stringify(value));
    };
    if (req.url !== '/demo' || req.method !== 'POST') return reply(404, {});
    if (!authorized(req.headers.authorization, token)) return reply(401, {});
    if (busy) return reply(409, {error: 'BUSY'});
    busy = true;
    try {
      let raw = '';
      for await (const chunk of req) {
        raw += chunk;
        if (raw.length > 1024) fail('BODY_TOO_LARGE');
      }
      await command(JSON.parse(raw));
      reply(200, {
        role,
        cycle: state.cycle || null,
        stopped: !!state.stopped,
        resetting: state.resetting || null,
        child_running: !!child,
      });
    } catch (e) {
      // Never emit provider errors or environment values.
      reply(409, {
        error: /^[A-Z_]+$/.test(e.code || e.message)
          ? e.code || e.message
          : 'DEMO_COMMAND_FAILED',
      });
    } finally {
      busy = false;
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(PORT, '127.0.0.1', resolve);
  });
  if (!state.stopped && !state.resetting) startChild();
  const watch = setInterval(() => {
    if (!closing && !busy && !child && !state.stopped && !state.resetting)
      startChild();
  }, 1000);
  async function close() {
    closing = true;
    clearInterval(watch);
    server.close();
    await stopChild();
    await store.close();
  }
  process.once('SIGTERM', () =>
    close().catch(() => {
      process.exitCode = 1;
    }),
  );
  process.once('SIGINT', () =>
    close().catch(() => {
      process.exitCode = 1;
    }),
  );
  return {close};
}

async function client(action, cycle, env = process.env) {
  const {token, role} = config(env);
  const r = await fetch(`http://127.0.0.1:${PORT}/demo`, {
    method: 'POST',
    signal: AbortSignal.timeout(30000),
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      action: action === 'ready' ? 'status' : action,
      cycle,
    }),
  });
  const body = await r.json();
  if (!r.ok) fail(body.error || 'DEMO_COMMAND_FAILED');
  if (action === 'ready') {
    if (!body.child_running || body.stopped || body.resetting)
      fail('CHILD_NOT_READY');
    const port =
      role === 'bot'
        ? env.BOT_METRICS_PORT || 9091
        : env.INGRESS_API_PORT || 8080;
    const health = await fetch(`http://127.0.0.1:${port}/healthz`, {
      signal: AbortSignal.timeout(3000),
    });
    if (!health.ok) fail('CHILD_NOT_READY');
  }
  return body;
}
if (require.main === module) {
  (process.argv[2] === 'serve'
    ? serve()
    : client(process.argv[2], process.argv[3]).then((x) =>
        console.log(JSON.stringify(x)),
      )
  ).catch(() => {
    console.error('DEMO_SUPERVISOR_FAILED');
    process.exitCode = 1;
  });
}
module.exports = {serve, client, openState, config, authorized, CYCLE};
