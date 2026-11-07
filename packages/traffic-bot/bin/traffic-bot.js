#!/usr/bin/env node
const {createServer} = require('node:http');
const {createTrafficBot, createHttpAdapter} = require('..');
const {DEFAULT_MAX_RECORDS, DEFAULT_MAX_LEDGER_BYTES} = require('../ledger');
const {LOCK_ERROR_CODES} = require('../owner-lock');
const {createControlRoutes, readControlToken, readInitialGeneration} =
  require('../control-api');

const MIN_API_TOKEN_LENGTH = 32;
/** RFC 6750 2.1 b64token: 1*( ALPHA / DIGIT / "-._~+/" ) *"=". */
const B64TOKEN_PATTERN = /^[A-Za-z0-9\-._~+/]+=*$/;
/** Exact WHATWG-normalized loopback literals; `localhost` may resolve away. */
const LOOPBACK_HOSTNAMES = ['127.0.0.1', '[::1]'];
/** Safe startup codes; they never carry the token or the URL. */
const CONFIG_ERROR_CODES = ['BOT_API_TOKEN_INVALID',
  'INSECURE_TOKEN_TRANSPORT', 'BOT_CONTROL_TOKEN_INVALID',
  'BOT_GENERATION_INVALID'];

function configError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function numeric(env, name, fallback, integer = true) {
  const value = env[name] === undefined ? fallback : Number(env[name]);
  if (!Number.isFinite(value) || value <= 0 ||
      (integer && !Number.isSafeInteger(value))) {
    throw new Error(`Invalid ${name}`);
  }
  return value;
}

/**
 * Returns BOT_API_TOKEN, or undefined when it is unset (unauthenticated
 * APIs keep working). A set but empty or malformed token is refused.
 */
function readBotApiToken(env) {
  const token = env.BOT_API_TOKEN;
  if (token === undefined) {
    return undefined;
  }
  if (token.length < MIN_API_TOKEN_LENGTH || !B64TOKEN_PATTERN.test(token)) {
    throw configError('BOT_API_TOKEN_INVALID');
  }
  return token;
}

/**
 * Whether a bearer token may travel to `baseUrl`: HTTPS, or plain HTTP to an
 * exact loopback literal, and never with certificate checks disabled through
 * NODE_TLS_REJECT_UNAUTHORIZED=0 in `env` or in this process's real env.
 */
function isTokenTransportAllowed(baseUrl, env = process.env) {
  if ([env, process.env].some(
      (source) => source.NODE_TLS_REJECT_UNAUTHORIZED === '0')) {
    return false;
  }
  const {protocol, hostname} = new URL(baseUrl);
  return protocol === 'https:' ||
    (protocol === 'http:' && LOOPBACK_HOSTNAMES.includes(hostname));
}

/** Adapter headers for `env`; checked before any ledger, socket or fetch. */
function authHeaders(env) {
  const token = readBotApiToken(env);
  if (token === undefined) {
    return {};
  }
  if (!isTokenTransportAllowed(env.BOT_BASE_URL, env)) {
    throw configError('INSECURE_TOKEN_TRANSPORT');
  }
  return {authorization: `Bearer ${token}`};
}

async function main(env = process.env) {
  const port = numeric(env, 'BOT_METRICS_PORT', 9091);
  if (port > 65535) throw new Error('Invalid BOT_METRICS_PORT');
  const headers = authHeaders(env);
  const controlToken = readControlToken(env);
  const generation = readInitialGeneration(env);
  const ratePerSecond = numeric(env, 'BOT_RATE', 1, false);
  const bot = await createTrafficBot({
    adapter: createHttpAdapter({baseUrl: env.BOT_BASE_URL,
      requestsPath: env.BOT_REQUESTS_PATH || '/api/requests', headers}),
    ledgerDir: env.BOT_LEDGER_DIR || './data/ledger',
    ratePerSecond,
    runId: env.BOT_RUN_ID,
    phase: env.BOT_PHASE || 'onprem_live',
    generation,
    maxPending: numeric(env, 'BOT_MAX_PENDING', 1000),
    maxRecords: numeric(env, 'BOT_MAX_RECORDS', DEFAULT_MAX_RECORDS),
    maxLedgerBytes: numeric(env, 'BOT_MAX_LEDGER_BYTES',
        DEFAULT_MAX_LEDGER_BYTES),
    maxRequestAgeMs: numeric(env, 'BOT_MAX_REQUEST_AGE_MS', 300000),
    maxAttempts: numeric(env, 'BOT_MAX_ATTEMPTS', 20),
    requestTimeoutMs: numeric(env, 'BOT_REQUEST_TIMEOUT_MS', 5000),
    retryDelayMs: numeric(env, 'BOT_RETRY_DELAY_MS', 1000),
    onSummary: (summary) =>
      process.stdout.write(`${JSON.stringify(summary)}\n`),
  });
  const handleControl = createControlRoutes({bot, token: controlToken,
    ratePerSecond, runId: env.BOT_RUN_ID});
  const server = createServer((req, res) => {
    if (handleControl(req, res)) return;
    if (req.url === '/metrics') {
      res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      return res.end(bot.metrics());
    }
    if (req.url === '/healthz') {
      const status = bot.snapshot();
      res.writeHead(status.fatal ? 503 : 200,
          {'Content-Type': 'application/json'});
      return res.end(JSON.stringify({status: status.fatal ? 'fail' : 'ok'}));
    }
    res.writeHead(404);
    res.end();
  });
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, env.BOT_METRICS_HOST || '0.0.0.0', resolve);
    });
  } catch (error) {
    await bot.stop();
    throw error;
  }
  let shutdown;
  async function stop() {
    if (shutdown) return shutdown;
    shutdown = (async () => {
      await bot.stop();
      await new Promise((resolve) => server.close(resolve));
    })();
    return shutdown;
  }
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  await bot.start();
  if (bot.snapshot().fatal) {
    process.exitCode = 1;
    await stop();
  }
}

if (require.main === module) {
  main().catch((error) => {
    // Raw messages may hold paths, URLs or secrets; print allow-listed codes.
    const code = [...LOCK_ERROR_CODES, ...CONFIG_ERROR_CODES]
        .includes(error?.code) ? error.code : null;
    console.error(code ? `traffic-bot startup failed: ${code}` :
      'traffic-bot startup or runtime failed; check configuration');
    process.exitCode = 1;
  });
}

module.exports = {main, numeric, readBotApiToken, isTokenTransportAllowed};
