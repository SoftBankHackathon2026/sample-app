const {randomUUID} = require('node:crypto');
const {setTimeout: delay} = require('node:timers/promises');
const {
  openLedger, positiveInteger, DEFAULT_MAX_RECORDS, DEFAULT_MAX_LEDGER_BYTES,
} = require('./ledger');
const {createHttpAdapter, HttpError} = require('./http-adapter');

const FIELDS = ['request_id', 'run_id', 'phase', 'request_no', 'payload',
  'created_at'];
const RUN_ID_PATTERN = /^[A-Za-z0-9_.-]{1,200}$/;
// The only lock failure that can surface after start; operators must see it.
const LOCK_LOST = 'LEDGER_LOCK_LOST';

function codedError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function terminalError(code) {
  const error = codedError(code);
  error.terminal = true;
  return error;
}

function validateRequest(request) {
  if (!request || FIELDS.some((key) => typeof request[key] !== 'string') ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
          request.request_id) ||
      !request.run_id || request.run_id.length > 200 ||
      !request.phase || request.phase.length > 100 ||
      Buffer.byteLength(request.payload) > 64 * 1024 ||
      FIELDS.some((key) => request[key].includes('\0')) ||
      !/^(0|[1-9]\d*)$/.test(request.request_no) ||
      BigInt(request.request_no) > 9223372036854775807n ||
      !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(request.created_at) ||
      !Number.isFinite(Date.parse(request.created_at)) ||
      new Date(request.created_at).toISOString() !== request.created_at) {
    throw terminalError('INVALID_REQUEST');
  }
  return Object.fromEntries(FIELDS.map((key) => [key, request[key]]));
}

/**
 * Check a committed envelope for this request. A malformed envelope proves
 * nothing (RESPONSE_INVALID); a well-formed record for the same ID with other
 * fields is a content conflict. Only a GET result may decide that conflict.
 */
function validateResponse(body, expected) {
  if (!body || body.status !== 'committed' || !body.request ||
      FIELDS.some((key) => typeof body.request[key] !== 'string') ||
      body.request.request_id !== expected.request_id ||
      !body.database || typeof body.database.name !== 'string' ||
      !body.database.name || body.database.name.length > 256 ||
      typeof body.environment !== 'string' || !body.environment ||
      body.environment.length > 128) {
    throw codedError('RESPONSE_INVALID');
  }
  if (FIELDS.some((key) => body.request[key] !== expected[key])) {
    throw terminalError('CONTENT_CONFLICT');
  }
  return {environment: body.environment, database: {name: body.database.name},
    observedAt: new Date().toISOString()};
}

function isAccepted(body, expected) {
  return body?.status === 'accepted' && body.request &&
    FIELDS.every((key) => body.request[key] === expected[key]);
}

function percentile(values, fraction) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * fraction) - 1];
}

function label(value) {
  return '"' + String(value).replace(/[\x00-\x09\x0b-\x1f\x7f]/g, '_')
      .replace(/\\/g, '\\\\').replace(/\n/g, '\\n')
      .replace(/"/g, '\\"') + '"';
}

/**
 * Durable single-writer load generator; storage and app transport are
 * separate.
 */
async function createTrafficBot({adapter, ledgerDir, ratePerSecond = 1,
  maxPending = 1000, maxRecords = DEFAULT_MAX_RECORDS,
  maxLedgerBytes = DEFAULT_MAX_LEDGER_BYTES, maxRequestAgeMs = 300000,
  maxAttempts = 20, requestTimeoutMs = 5000, retryDelayMs = 1000, runId,
  phase = 'onprem_live', generation = 'running', onSummary = () => {}} = {}) {
  if (!adapter || typeof adapter.write !== 'function' ||
      typeof adapter.read !== 'function') {
    throw new TypeError('adapter.write and adapter.read are required');
  }
  if (!Number.isFinite(ratePerSecond) || ratePerSecond <= 0 ||
      ratePerSecond > 1000) {
    throw new TypeError(
        'ratePerSecond must be greater than 0 and at most 1000');
  }
  for (const [name, value] of Object.entries({maxRequestAgeMs, maxAttempts,
    requestTimeoutMs, retryDelayMs})) {
    positiveInteger(value, name);
  }
  // Each writer needs its own run ID; it is never generated or defaulted.
  if (typeof runId !== 'string' || !RUN_ID_PATTERN.test(runId)) {
    throw new TypeError('runId must match ^[A-Za-z0-9_.-]{1,200}$');
  }
  if (typeof phase !== 'string' || !phase || phase.length > 100) {
    throw new TypeError('phase must be a bounded nonempty string');
  }
  if (generation !== 'running' && generation !== 'stopped') {
    throw new TypeError('generation must be running or stopped');
  }
  const ledger = await openLedger({dir: ledgerDir, maxPending, maxRecords,
    maxLedgerBytes, runId});
  try {
    for (const entry of ledger.entries()) {
      validateRequest(entry.request);
    }
  } catch (error) {
    await ledger.close();
    throw error;
  }
  let stopped = false;
  let paused = false;
  // Stopping generation only stops new requests; delivery keeps settling the
  // ledger so a stopped bot leaves nothing pending behind.
  let generating = generation === 'running';
  let running = false;
  let processing;
  let loop;
  let deliveryLoop;
  let summaryTimer;
  let generated = ledger.entries().filter((entry) =>
    entry.request.run_id === runId).reduce((last, entry) =>
      BigInt(entry.request.request_no) > last ?
        BigInt(entry.request.request_no) : last, 0n);
  const controllers = new Set();
  const counters = {write_attempts: 0, write_errors: 0, read_attempts: 0,
    read_errors: 0, generation_errors: 0, generation_blocked: 0};
  const latencies = [];
  const environments = new Map();
  let fatalCode = null;
  let finalSnapshot;
  const wake = new AbortController();

  function snapshot() {
    if (finalSnapshot) return structuredClone(finalSnapshot);
    return {
      at: new Date().toISOString(), paused, running,
      generation: generating ? 'running' : 'stopped',
      ...ledger.summary(),
      ...counters, fatal: fatalCode,
      latency_ms: {p95: percentile(latencies, 0.95),
        p99: percentile(latencies, 0.99), max: Math.max(0, ...latencies)},
      environments: Object.fromEntries([...environments].map(([name, count]) =>
        [name, {count, ratio: count / Math.max(1,
          [...environments.values()].reduce((a, b) => a + b, 0))}])),
    };
  }

  async function call(method, argument) {
    const controller = new AbortController();
    controllers.add(controller);
    const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
    const aborted = new Promise((resolve, reject) => {
      controller.signal.addEventListener('abort', () => {
        const error = new Error('REQUEST_ABORTED');
        error.code = 'REQUEST_ABORTED';
        reject(error);
      }, {once: true});
    });
    try {
      return await Promise.race([
        adapter[method](argument, {signal: controller.signal}), aborted,
      ]);
    } finally {
      clearTimeout(timer);
      controllers.delete(controller);
    }
  }

  async function commit(entry, response, priorObservation) {
    const observation = validateResponse(response, entry.request);
    const elapsed = Date.now() - entry.enqueuedAt;
    await ledger.update(entry.request.request_id, {
      state: 'committed', completedAt: Date.now(), lastError: null,
      observations: [
        ...(priorObservation ? [priorObservation] : []), observation],
    });
    latencies.push(elapsed);
    if (latencies.length > 1000) latencies.shift();
    const key = environments.has(observation.environment) ||
      environments.size < 64 ? observation.environment : 'other';
    environments.set(key, (environments.get(key) || 0) + 1);
  }

  async function reconcile(entry) {
    counters.read_attempts++;
    try {
      return await call('read', entry.request.request_id);
    } catch (error) {
      counters.read_errors++;
      // A failed read never proves that an earlier POST was not stored.
      const unconfirmed = codedError(error.code);
      unconfirmed.retryAfterMs = error.retryAfterMs;
      throw unconfirmed;
    }
  }

  /**
   * Settle a possibly committed POST by reading the same ID. Resolves true
   * once committed, false if absent; throws CONTENT_CONFLICT (final) or an
   * unconfirmed error.
   */
  async function resolveByRead(entry, postObservation) {
    const found = await reconcile(entry);
    if (!found) return false;
    if (isAccepted(found, entry.request)) {
      await ledger.update(entry.request.request_id, {
        acceptedAt: entry.acceptedAt || Date.now(),
        nextAttemptAt: Date.now() + retryDelayMs, lastError: null,
      });
      return true; // Still pending; acceptance is never counted as committed.
    }
    await commit(entry, found, postObservation);
    return true;
  }

  async function reconcileFinal(entry, code) {
    try {
      if (await resolveByRead(entry)) return;
    } catch (error) {
      if (error.terminal && error.code === 'CONTENT_CONFLICT') {
        return failOrRetry(entry, error, true);
      }
    }
    if (!stopped) {
      await ledger.update(entry.request.request_id, {state: 'unknown',
        lastError: code, completedAt: Date.now()});
    }
  }

  async function processEntry(entry) {
    if (entry.acceptedAt) {
      // The server owns an accepted request. Poll it; do not expire or POST
      // again merely because migration takes longer than the retry budget.
      try {
        if (await resolveByRead(entry)) return;
        return failOrRetry(entry, codedError('ACCEPTED_REQUEST_MISSING'));
      } catch (error) {
        return failOrRetry(entry, error, Boolean(error.terminal));
      }
    }
    if (Date.now() - entry.enqueuedAt >= maxRequestAgeMs) {
      if (entry.attempts > 0) {
        return reconcileFinal(entry, 'REQUEST_EXPIRED_UNCONFIRMED');
      }
      await ledger.update(entry.request.request_id, {
        state: 'failed', lastError: 'REQUEST_EXPIRED', completedAt: Date.now(),
      });
      return;
    }
    // A prior attempt could have committed even if its response was lost.
    if (entry.attempts > 0) {
      try {
        if (await resolveByRead(entry)) return;
      } catch (error) {
        return failOrRetry(entry, error, Boolean(error.terminal));
      }
    }
    if (entry.attempts >= maxAttempts) {
      await ledger.update(entry.request.request_id, {state: 'unknown',
        lastError: 'ATTEMPTS_EXHAUSTED_UNCONFIRMED', completedAt: Date.now()});
      return;
    }
    const attempts = entry.attempts + 1;
    await ledger.update(entry.request.request_id, {attempts});
    entry = {...entry, attempts};
    counters.write_attempts++;
    let result;
    try {
      result = await call('write', entry.request);
    } catch (error) {
      counters.write_errors++;
      return handleWriteError(entry, error);
    }
    return confirmWrite(entry, result);
  }

  async function confirmWrite(entry, result) {
    if (isAccepted(result, entry.request)) {
      await ledger.update(entry.request.request_id, {
        acceptedAt: Date.now(), nextAttemptAt: Date.now() + retryDelayMs,
        lastError: null,
      });
      return;
    }
    let observation;
    let postError;
    try {
      observation = validateResponse(result, entry.request);
    } catch (error) {
      // A 2xx body that fails validation says nothing about the stored row.
      counters.write_errors++;
      postError = codedError('RESPONSE_INVALID');
    }
    try {
      if (await resolveByRead(entry, observation)) return;
    } catch (error) {
      // Only validation of the GET record can prove a content conflict.
      return failOrRetry(entry,
          error.code === 'CONTENT_CONFLICT' || !postError ? error : postError,
          Boolean(error.terminal));
    }
    return failOrRetry(entry, postError || codedError('READ_NOT_FOUND'));
  }

  async function handleWriteError(entry, error) {
    if (stopped) return;
    if (error instanceof HttpError && error.status === 503 &&
        error.notCommitted === true) {
      // This known refusal did not consume a potentially committed POST
      // attempt.
      const refunded = {...entry, attempts: entry.attempts - 1};
      await ledger.update(entry.request.request_id,
          {attempts: refunded.attempts});
      return failOrRetry(refunded, error);
    }
    // This refusal proves only this POST did not commit. With no prior
    // uncertain POST (pause refunds excluded), it also settles the request.
    if (entry.attempts === 1 && error.terminal && !error.afterResponse) {
      return failOrRetry(entry, error, true);
    }
    try {
      if (await resolveByRead(entry)) return;
    } catch (cause) {
      if (cause.terminal && cause.code === 'CONTENT_CONFLICT') {
        return failOrRetry(entry, cause, true);
      }
    }
    return failOrRetry(entry, error);
  }

  async function failOrRetry(entry, error, confirmedFailure = false) {
    if (stopped) return;
    // Adapter errors are untrusted and may contain secrets; persist fixed
    // codes.
    const safeCodes = ['RESPONSE_INVALID', 'CONTENT_CONFLICT',
      'INVALID_JSON_RESPONSE', 'RESPONSE_TOO_LARGE', 'HTTP_TRANSPORT_ERROR',
      'REQUEST_ABORTED', 'WRITES_PAUSED'];
    safeCodes.push('ACCEPTED_REQUEST_MISSING');
    // The caller identifies proof from the first POST or a validated GET.
    // A later POST refusal or any GET failure cannot settle prior writes.
    const failed = confirmedFailure;
    const code = safeCodes.includes(error.code) ||
      /^HTTP_[0-9]{3}$/.test(error.code || '') ? error.code :
      (failed ? 'PERMANENT_REQUEST_FAILURE' : 'TRANSIENT_REQUEST_FAILURE');
    const now = Date.now();
    const retryAfterMs = Number.isFinite(error.retryAfterMs) &&
        error.retryAfterMs >= 0 ? Math.ceil(error.retryAfterMs) : 0;
    const retryInMs = Math.min(maxRequestAgeMs, Number.MAX_SAFE_INTEGER - now,
        Math.max(retryDelayMs, retryAfterMs));
    await ledger.update(entry.request.request_id, {
      state: failed ? 'failed' : 'pending', lastError: code,
      nextAttemptAt: now + retryInMs,
      ...(failed ? {completedAt: Date.now()} : {}),
    });
  }

  function processPending() {
    if (processing) return processing;
    processing = (async () => {
      for (const entry of ledger.pendingEntries()) {
        if (stopped || paused) break;
        if (entry.state === 'pending' && entry.nextAttemptAt <= Date.now()) {
          await processEntry(entry);
        }
      }
    })().finally(() => { processing = null; });
    return processing;
  }

  async function enqueue(request) {
    if (stopped) throw new Error('bot stopped');
    await ledger.add(validateRequest(request));
  }

  async function generate() {
    const sequence = generated + 1n;
    await enqueue({request_id: randomUUID(), run_id: runId, phase,
      request_no: String(sequence), payload: `synthetic-${sequence}`,
      created_at: new Date().toISOString()});
    generated = sequence;
  }

  function report() {
    try {
      onSummary(snapshot());
    } catch (error) {
      fatalCode = 'SUMMARY_CALLBACK_FAILED';
    }
  }

  return {
    enqueue, processPending, snapshot,
    async pause() {
      paused = true;
      if (processing) await processing;
    },
    resume() { paused = false; },
    setGeneration(enabled) {
      if (typeof enabled !== 'boolean') {
        throw new TypeError('enabled must be a boolean');
      }
      generating = enabled;
      return generating ? 'running' : 'stopped';
    },
    start() {
      if (running || stopped) throw new Error('bot already started or stopped');
      running = true;
      summaryTimer = setInterval(report, 1000);
      loop = (async () => {
        while (!stopped && running) {
          const started = Date.now();
          try {
            // A stopped generator only skips this tick; delivery has its own loop.
            if (generating) {
              if (ledger.summary().pending >= maxPending) {
                counters.generation_blocked++;
              } else {
                await generate();
              }
            }
          } catch (error) {
            if (error.code === 'PENDING_LIMIT') {
              counters.generation_blocked++;
            } else {
              counters.generation_errors++;
              fatalCode = error.code === LOCK_LOST ? LOCK_LOST :
                'LEDGER_GENERATION_LIMIT_OR_IO_ERROR';
              report();
              running = false;
              break;
            }
          }
          if (!stopped) {
            await delay(Math.max(1,
                1000 / ratePerSecond - (Date.now() - started)), undefined,
            {signal: wake.signal}).catch((error) => {
              if (error.name !== 'AbortError') throw error;
            });
          }
        }
      })().catch((error) => {
        fatalCode = error.code === LOCK_LOST ? LOCK_LOST :
          'LEDGER_PROCESSING_FAILED';
        running = false;
        report();
      });
      // Generation stays independent of read latency and accumulated backlog.
      deliveryLoop = (async () => {
        while (!stopped && running) {
          await processPending();
          await delay(10, undefined, {signal: wake.signal}).catch((error) => {
            if (error.name !== 'AbortError') throw error;
          });
        }
      })().catch((error) => {
        fatalCode = error.code === LOCK_LOST ? LOCK_LOST : 'LEDGER_PROCESSING_FAILED';
        running = false; report();
      });
      return Promise.all([loop, deliveryLoop]);
    },
    metrics() {
      const value = snapshot();
      const lines = [
        '# HELP traffic_bot_requests_total Durable request outcomes',
        '# TYPE traffic_bot_requests_total gauge',
        ...['pending', 'accepted_pending', 'committed', 'failed', 'unknown'].map((state) =>
          `traffic_bot_requests_total{state=${label(state)}} ${value[state]}`),
        '# TYPE traffic_bot_attempts_total counter',
        ...Object.entries(counters).map(([kind, count]) =>
          `traffic_bot_attempts_total{kind=${label(kind)}} ${count}`),
        '# TYPE traffic_bot_commit_latency_ms summary',
        'traffic_bot_commit_latency_ms{quantile="0.95"} ' +
          `${value.latency_ms.p95}`,
        'traffic_bot_commit_latency_ms{quantile="0.99"} ' +
          `${value.latency_ms.p99}`,
        '# TYPE traffic_bot_environment_commits_total counter',
        ...[...environments].map(([environment, count]) =>
          'traffic_bot_environment_commits_total' +
          `{environment=${label(environment)}} ${count}`),
        '# TYPE traffic_bot_paused gauge',
        `traffic_bot_paused ${paused ? 1 : 0}`,
        '# TYPE traffic_bot_generating gauge',
        `traffic_bot_generating ${generating ? 1 : 0}`,
      ];
      return `${lines.join('\n')}\n`;
    },
    async stop() {
      if (stopped) return;
      stopped = true;
      running = false;
      clearInterval(summaryTimer);
      wake.abort();
      for (const controller of controllers) {
        controller.abort();
      }
      if (loop) await loop;
      if (deliveryLoop) await deliveryLoop;
      if (processing) await processing;
      finalSnapshot = snapshot();
      await ledger.close();
    },
  };
}

module.exports = {createTrafficBot, createHttpAdapter, openLedger, HttpError};
