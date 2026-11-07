const {readFileSync} = require('node:fs');
const {RequestStoreError} = require('./request');

const AUTH_MODES = ['password', 'rds-iam'];
const DEFAULT_POSTGRES_PORT = 5432;
// URL query keys RDS IAM mode hands to pg. Any other key would land in the
// pool options, where connectionString, host or store options could replace
// the token callback, the TLS settings or the store's own behavior.
const IAM_URL_QUERY_KEYS = ['application_name', 'options'];

function configuration(message) {
  return new RequestStoreError('INVALID_CONFIGURATION', message, 500);
}

function positiveInteger(value, fallback, max) {
  if (value === undefined || value === '') return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number < 1 || number > max) {
    throw configuration('PostgreSQL limits must be positive bounded integers');
  }
  return number;
}

/**
 * True for URL query keys that pg-connection-string turns into TLS settings
 * (every ssl* key and uselibpqcompat), compared case-insensitively.
 * @param {string} name
 * @return {boolean}
 */
function isUrlTlsKey(name) {
  const key = name.toLowerCase();
  return key.startsWith('ssl') || key === 'uselibpqcompat';
}

/**
 * Decodes a URL user or database name. URIError would escape the
 * configuration error contract, so it becomes INVALID_CONFIGURATION.
 * @param {string} part Percent-encoded URL component.
 * @return {string}
 */
function decodeUrlPart(part) {
  try {
    return decodeURIComponent(part);
  } catch {
    throw configuration('DATABASE_URL has invalid percent-encoding');
  }
}

/**
 * Returns the allow-listed URL query keys as pg options.
 * @param {!URL} url
 * @return {!Object<string, string>}
 */
function iamQueryOptions(url) {
  const rejected = [...url.searchParams.keys()].find(
      (key) => !IAM_URL_QUERY_KEYS.includes(key));
  if (rejected !== undefined) {
    // Only the key is named; values may hold credentials or URLs.
    throw configuration(
        `RDS IAM mode rejects DATABASE_URL query key ${rejected}`);
  }
  return Object.fromEntries(url.searchParams);
}

/**
 * Builds the AWS SDK RDS signer. Loaded on demand so password mode never
 * pulls in the SDK.
 * @param {{hostname: string, port: number, username: string,
 *     region: string}} config
 * @return {{getAuthToken: function(): !Promise<string>}}
 */
function awsRdsSigner(config) {
  const {Signer} = require('@aws-sdk/rds-signer');
  return new Signer(config);
}

/**
 * Pool options that authenticate with an RDS IAM token instead of a password.
 * The URL becomes discrete fields because pg lets a connection string or
 * PGPASSWORD replace a password callback. Each new connection signs a fresh
 * token; RDS checks it only at login, so its 15 minute lifetime does not end
 * open connections.
 * @param {!URL} url DATABASE_URL without sslmode.
 * @param {!Object<string, string>} env
 * @param {function(!Object): {getAuthToken: function(): !Promise<string>}}
 *     signerFactory
 * @return {!Object} host, port, user, database, allowed query keys and
 *     password.
 */
function rdsIamOptions(url, env, signerFactory) {
  if (url.password || url.searchParams.has('password')) {
    throw configuration('RDS IAM mode rejects a DATABASE_URL password');
  }
  const user = decodeUrlPart(url.username);
  if (!user) {
    throw configuration('RDS IAM mode requires a DATABASE_URL user');
  }
  if (!env.AWS_REGION) {
    throw configuration('RDS IAM mode requires AWS_REGION');
  }
  const query = iamQueryOptions(url);
  const database = decodeUrlPart(url.pathname.slice(1));
  const host = url.hostname;
  const port = url.port ? Number(url.port) : DEFAULT_POSTGRES_PORT;
  const signer = signerFactory(
      {hostname: host, port, username: user, region: env.AWS_REGION});
  return {
    ...query, host, port, user, database,
    password: () => signer.getAuthToken(),
  };
}

/**
 * Build explicit TLS settings without URL SSL parameters overriding them.
 * DB_AUTH_MODE=rds-iam swaps the URL password for an RDS IAM token callback.
 * @param {!Object<string, string>=} env
 * @param {{signerFactory: (function(!Object): {getAuthToken: function():
 *     !Promise<string>}|undefined)}=} dependencies Signer override for tests.
 * @return {!Object}
 */
function postgresOptionsFromEnv(env = process.env,
    {signerFactory = awsRdsSigner} = {}) {
  const authMode = env.DB_AUTH_MODE || 'password';
  if (!AUTH_MODES.includes(authMode)) {
    throw configuration('DB_AUTH_MODE must be password or rds-iam');
  }
  if (!env.DATABASE_URL) {
    throw configuration('DATABASE_URL is required');
  }
  let url;
  try {
    url = new URL(env.DATABASE_URL);
  } catch {
    throw configuration('DATABASE_URL must be a PostgreSQL URL');
  }
  if (!['postgres:', 'postgresql:'].includes(url.protocol)) {
    throw configuration('DATABASE_URL must be a PostgreSQL URL');
  }
  const mode = env.PGSSLMODE || url.searchParams.get('sslmode') ||
      (env.DB_SSL === 'true' ? 'verify-full' : 'disable');
  if (mode === 'disable' && (env.DB_SSL === 'true' ||
      env.PGSSLROOTCERT || env.DB_SSL_CA)) {
    throw configuration('TLS-required settings conflict with disabled TLS');
  }
  if (!['disable', 'require', 'verify-ca', 'verify-full'].includes(mode)) {
    throw configuration('TLS mode must disable TLS or verify certificates');
  }
  if ([...url.searchParams.keys()].some(
      (name) => name !== 'sslmode' && isUrlTlsKey(name))) {
    throw configuration('Use PGSSLROOTCERT for TLS and remove URL SSL options');
  }
  url.searchParams.delete('sslmode');
  const ssl = mode === 'disable' ? false : {rejectUnauthorized: true};
  if (ssl && env.PGSSLROOTCERT) {
    ssl.ca = readFileSync(env.PGSSLROOTCERT, 'utf8');
  } else if (ssl && env.DB_SSL_CA) {
    ssl.ca = env.DB_SSL_CA;
  }
  if (authMode === 'rds-iam' && !ssl) {
    throw configuration('RDS IAM mode requires verified TLS');
  }
  const target = authMode === 'rds-iam' ?
      rdsIamOptions(url, env, signerFactory) :
      {connectionString: url.toString()};
  return {
    ...target, ssl,
    connectionTimeoutMillis: positiveInteger(env.DB_CONNECT_TIMEOUT_MS, 3000,
        60000),
    statementTimeoutMillis: positiveInteger(env.DB_STATEMENT_TIMEOUT_MS, 5000,
        60000),
    max: positiveInteger(env.DB_POOL_MAX, 5, 100),
  };
}

module.exports = {isUrlTlsKey, postgresOptionsFromEnv};
