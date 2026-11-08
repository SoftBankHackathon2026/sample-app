// Operator control of request generation on the metrics port.
//   GET  /control             current generation state and ledger counts
//   POST /control/generation  {"enabled": boolean}, Bearer BOT_CONTROL_TOKEN
// Without BOT_CONTROL_TOKEN the POST route is closed (403), so existing
// deployments keep their behaviour. Stopping generation never touches
// delivery: requests already in the ledger are still settled.
const {timingSafeEqual} = require('node:crypto');

const MIN_CONTROL_TOKEN_LENGTH = 32;
const CONTROL_TOKEN_PATTERN = /^[A-Za-z0-9\-._~+/]+=*$/;
const MAX_BODY_BYTES = 1024;
const GENERATION_STATES = ['running', 'stopped'];

function configError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

/** BOT_CONTROL_TOKEN, or undefined when unset. A malformed token is refused. */
function readControlToken(env) {
  const token = env.BOT_CONTROL_TOKEN;
  if (token === undefined) return undefined;
  if (token.length < MIN_CONTROL_TOKEN_LENGTH ||
      !CONTROL_TOKEN_PATTERN.test(token)) {
    throw configError('BOT_CONTROL_TOKEN_INVALID');
  }
  return token;
}

/** BOT_GENERATION: whether the bot generates requests right after start. */
function readInitialGeneration(env) {
  const value = env.BOT_GENERATION ?? 'running';
  if (!GENERATION_STATES.includes(value)) {
    throw configError('BOT_GENERATION_INVALID');
  }
  return value;
}

function sendJson(res, status, body) {
  res.writeHead(status, {'Content-Type': 'application/json'});
  res.end(JSON.stringify(body));
}

function bearerMatches(header, token) {
  if (typeof header !== 'string' || !header.startsWith('Bearer ')) return false;
  const given = Buffer.from(header.slice('Bearer '.length));
  const expected = Buffer.from(token);
  return given.length === expected.length && timingSafeEqual(given, expected);
}

function readSmallBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > MAX_BODY_BYTES) {
        reject(configError('BODY_TOO_LARGE'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

/**
 * Returns a request handler that answers the control routes and returns
 * false for any other path so the caller can keep serving /metrics.
 */
function createControlRoutes({bot, token, ratePerSecond, runId,
  requestEndpoint}) {
  function view() {
    const s = bot.snapshot();
    return {generation: s.generation, pending: s.pending, committed: s.committed,
      failed: s.failed, unknown: s.unknown, accepted_pending: s.accepted_pending,
      paused: s.paused, fatal: s.fatal, generated_total: s.generated_total,
      generation_limit: s.generation_limit, ledger_id: s.ledger_id,
      control_enabled: token !== undefined, request_endpoint: requestEndpoint,
      rate_per_second: ratePerSecond, run_id: runId};
  }

  async function setGeneration(req, res) {
    if (token === undefined) {
      return sendJson(res, 403, {error: 'CONTROL_DISABLED'});
    }
    if (!bearerMatches(req.headers.authorization, token)) {
      return sendJson(res, 401, {error: 'UNAUTHORIZED'});
    }
    let body;
    try {
      body = JSON.parse(await readSmallBody(req));
    } catch {
      return sendJson(res, 400, {error: 'BAD_JSON'});
    }
    if (typeof body?.enabled !== 'boolean') {
      return sendJson(res, 400, {error: 'ENABLED_MUST_BE_BOOLEAN'});
    }
    if (body.limit !== undefined &&
        (!Number.isSafeInteger(body.limit) || body.limit < 0)) {
      return sendJson(res, 400, {error: 'INVALID_GENERATION_LIMIT'});
    }
    const current = view();
    if ((body.expected_run_id !== undefined &&
          body.expected_run_id !== current.run_id) ||
        (body.expected_ledger_id !== undefined &&
          body.expected_ledger_id !== current.ledger_id)) {
      return sendJson(res, 409, {error: 'BOT_IDENTITY_CONFLICT'});
    }
    try {
      await bot.setGeneration(body.enabled, {limit: body.limit});
    } catch (error) {
      if (error.code === 'GENERATION_LIMIT_BELOW_GENERATED') {
        return sendJson(res, 409, {error: error.code});
      }
      return sendJson(res, 503, {error: 'GENERATION_CONTROL_FAILED'});
    }
    return sendJson(res, 200, view());
  }

  return function handleControl(req, res) {
    if (req.url === '/control' && req.method === 'GET') {
      sendJson(res, 200, view());
      return true;
    }
    if (req.url === '/control/generation') {
      if (req.method !== 'POST') {
        sendJson(res, 405, {error: 'METHOD_NOT_ALLOWED'});
      } else {
        setGeneration(req, res).catch(() => {
          if (!res.headersSent) sendJson(res, 400, {error: 'BAD_REQUEST'});
        });
      }
      return true;
    }
    return false;
  };
}

module.exports = {createControlRoutes, readControlToken, readInitialGeneration};
