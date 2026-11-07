const {test} = require('node:test');
const assert = require('node:assert/strict');
const {inspect} = require('node:util');
const {Client} = require('pg');
const {
  createPostgresRequestStore, postgresOptionsFromEnv,
} = require('../packages/request-store');

const IAM_ENV = {
  DATABASE_URL: 'postgres://app_iam@demo.abc.ap-northeast-2.rds.amazonaws.com' +
      ':6543/demo?application_name=bot',
  DB_AUTH_MODE: 'rds-iam',
  AWS_REGION: 'ap-northeast-2',
  PGSSLMODE: 'verify-full',
  DB_SSL_CA: 'RDS CA PEM',
};

/**
 * Records every signer configuration and hands out a fresh token per call,
 * so a test can tell one token per connection from a cached one.
 * @return {{signerFactory: function(!Object): !Object,
 *     configs: !Array<!Object>, tokens: !Array<string>}}
 */
function recordingSigner() {
  const configs = [];
  const tokens = [];
  return {
    configs, tokens,
    signerFactory: (config) => {
      configs.push(config);
      return {
        async getAuthToken() {
          const token = `token-${tokens.length + 1}`;
          tokens.push(token);
          return token;
        },
      };
    },
  };
}

test('default mode keeps the password URL and no password callback', () => {
  const env = {DATABASE_URL: 'postgres://user:pass@db/demo'};
  for (const mode of [undefined, '', 'password']) {
    const options = postgresOptionsFromEnv({...env, DB_AUTH_MODE: mode});
    assert.equal(options.connectionString, 'postgres://user:pass@db/demo');
    assert.equal(options.password, undefined, String(mode));
    assert.equal(options.host, undefined, String(mode));
  }
});

test('RDS IAM mode signs a new token for every connection', async () => {
  const signer = recordingSigner();
  const options = postgresOptionsFromEnv(IAM_ENV, signer);
  assert.equal(options.connectionString, undefined);
  assert.equal(options.host, 'demo.abc.ap-northeast-2.rds.amazonaws.com');
  assert.equal(options.port, 6543);
  assert.equal(options.user, 'app_iam');
  assert.equal(options.database, 'demo');
  assert.equal(options.application_name, 'bot');
  assert.deepEqual(options.ssl, {rejectUnauthorized: true, ca: 'RDS CA PEM'});
  assert.equal(await options.password(), 'token-1');
  assert.equal(await options.password(), 'token-2');
  assert.deepEqual(signer.configs, [{
    hostname: 'demo.abc.ap-northeast-2.rds.amazonaws.com', port: 6543,
    username: 'app_iam', region: 'ap-northeast-2',
  }]);
});

test('RDS IAM mode defaults the port and decodes URL parts', async () => {
  const signer = recordingSigner();
  const options = postgresOptionsFromEnv({...IAM_ENV,
    DATABASE_URL: 'postgresql://app%2Biam@db.example/demo%20db'}, signer);
  assert.equal(options.port, 5432);
  assert.equal(options.user, 'app+iam');
  assert.equal(options.database, 'demo db');
  await options.password();
  assert.equal(signer.configs[0].username, 'app+iam');
  assert.equal(signer.configs[0].port, 5432);
});

test('the password callback survives into pg.Client despite PGPASSWORD',
    async (t) => {
      const saved = process.env.PGPASSWORD;
      t.after(() => restoreEnv({PGPASSWORD: saved}));
      process.env.PGPASSWORD = 'env-password-must-not-win';
      let config;
      const store = createPostgresRequestStore({
        ...postgresOptionsFromEnv(IAM_ENV, recordingSigner()),
        poolFactory: (poolConfig) => {
          config = poolConfig;
          return {on() {}, end: async () => {}};
        },
      });
      // pg would replace the callback with URL or PGPASSWORD values.
      const client = new Client(config);
      assert.equal(typeof client.password, 'function');
      assert.equal(await client.password(), 'token-1');
      assert.equal(client.connectionParameters.user, 'app_iam');
      assert.equal(client.connectionParameters.port, 6543);
      await store.close();
    });

test('RDS IAM mode never falls back to a password in the URL', () => {
  for (const url of [
    'postgres://app_iam:url-secret-1@db.example/demo',
    'postgres://app_iam@db.example/demo?password=url-secret-1',
  ]) {
    let error;
    try {
      postgresOptionsFromEnv({...IAM_ENV, DATABASE_URL: url},
          recordingSigner());
    } catch (caught) {
      error = caught;
    }
    assert.equal(error?.code, 'INVALID_CONFIGURATION', url);
    assert.match(error.message, /password/);
    assert.doesNotMatch(inspect(error, {showHidden: true, depth: 5}),
        /url-secret-1/);
  }
});

test('RDS IAM mode rejects missing settings and disabled TLS', () => {
  const cases = [
    ['no user', {DATABASE_URL: 'postgres://db.example/demo'}, /user/],
    ['no host', {DATABASE_URL: 'postgres://app_iam@/demo'}, /DATABASE_URL/],
    ['bad user encoding', {DATABASE_URL: 'postgres://app%E0@db.example/demo'},
      /DATABASE_URL/],
    ['bad database encoding',
      {DATABASE_URL: 'postgres://app_iam@db.example/demo%E0'}, /DATABASE_URL/],
    ['no region', {AWS_REGION: undefined}, /AWS_REGION/],
    ['empty region', {AWS_REGION: ''}, /AWS_REGION/],
    ['TLS off', {PGSSLMODE: 'disable', DB_SSL_CA: undefined}, /TLS/],
    ['TLS unset', {PGSSLMODE: undefined, DB_SSL_CA: undefined}, /TLS/],
  ];
  for (const [label, override, message] of cases) {
    assert.throws(() => postgresOptionsFromEnv({...IAM_ENV, ...override},
        recordingSigner()), {code: 'INVALID_CONFIGURATION', message}, label);
  }
});

test('unknown DB_AUTH_MODE values are rejected instead of guessed', () => {
  for (const mode of ['rds_iam', 'RDS-IAM', 'iam', 'cloudsql-iam']) {
    assert.throws(() => postgresOptionsFromEnv({...IAM_ENV,
      DB_AUTH_MODE: mode}, recordingSigner()),
    {code: 'INVALID_CONFIGURATION', message: /DB_AUTH_MODE/}, mode);
  }
});

test('signer failures reach the connection instead of being swallowed',
    async () => {
      const failure = new Error('no credentials');
      const options = postgresOptionsFromEnv(IAM_ENV, {
        signerFactory: () => ({getAuthToken: async () => {
          throw failure;
        }}),
      });
      await assert.rejects(options.password(), failure);
    });

test('the default AWS SDK signer presigns for the URL user and endpoint',
    async (t) => {
      // Static keys keep the SDK credential chain offline.
      const keys = {AWS_ACCESS_KEY_ID: 'AKIDEXAMPLE',
        AWS_SECRET_ACCESS_KEY: 'example-secret', AWS_SESSION_TOKEN: undefined};
      const saved = Object.fromEntries(
          Object.keys(keys).map((name) => [name, process.env[name]]));
      t.after(() => restoreEnv(saved));
      restoreEnv(keys);
      const token = await postgresOptionsFromEnv(IAM_ENV).password();
      const signed = new URL(`https://${token}`);
      assert.equal(signed.host,
          'demo.abc.ap-northeast-2.rds.amazonaws.com:6543');
      assert.equal(signed.searchParams.get('Action'), 'connect');
      assert.equal(signed.searchParams.get('DBUser'), 'app_iam');
      assert.match(signed.searchParams.get('X-Amz-Credential'),
          /\/ap-northeast-2\/rds-db\/aws4_request$/);
      assert.equal(signed.searchParams.get('X-Amz-Expires'), '900');
    });

/**
 * Sets or deletes process.env entries; undefined deletes the entry.
 * @param {!Object<string, (string|undefined)>} values
 */
function restoreEnv(values) {
  for (const [name, value] of Object.entries(values)) {
    if (value === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = value;
    }
  }
}

test('RDS IAM mode forwards only allow-listed URL query keys', () => {
  const options = postgresOptionsFromEnv({...IAM_ENV, DATABASE_URL:
    'postgres://app_iam@db.example/demo?application_name=bot&options=-c%20x'},
  recordingSigner());
  assert.equal(options.application_name, 'bot');
  assert.equal(options.options, '-c x');
  for (const [key, value] of [
    ['writesPaused', '0'], ['poolFactory', 'x'], ['host', 'other.example'],
    ['user', 'app_password'], ['statementTimeoutMillis', '1'],
  ]) {
    assert.throws(() => postgresOptionsFromEnv({...IAM_ENV,
      DATABASE_URL: `postgres://app_iam@db.example/demo?${key}=${value}`},
    recordingSigner()), (error) => {
      assert.equal(error.code, 'INVALID_CONFIGURATION', key);
      assert.match(error.message, new RegExp(`\\b${key}\\b`));
      assert.doesNotMatch(error.message, new RegExp(`=${value}`));
      return true;
    });
  }
});

test('a nested connectionString query cannot bring back a password login',
    (t) => {
      const saved = process.env.PGPASSWORD;
      t.after(() => restoreEnv({PGPASSWORD: saved}));
      process.env.PGPASSWORD = 'env-password-must-not-win';
      for (const nested of [
        'postgres://app_iam:nested-secret-1@db.example/demo',
        'postgres://app_iam@db.example/demo',
        'postgres://app_iam@db.example/demo?sslmode=disable',
        'postgres://app_iam@db.example/demo?sslmode=no-verify',
      ]) {
        let error;
        try {
          postgresOptionsFromEnv({...IAM_ENV, DATABASE_URL:
            'postgres://app_iam@db.example/demo?connectionString=' +
            encodeURIComponent(nested)}, recordingSigner());
        } catch (caught) {
          error = caught;
        }
        assert.equal(error?.code, 'INVALID_CONFIGURATION', nested);
        assert.match(error.message, /connectionString/);
        assert.doesNotMatch(inspect(error, {showHidden: true, depth: 5}),
            /nested-secret-1|sslmode/);
      }
    });

test('password mode never loads the AWS SDK', () => {
  const {spawnSync} = require('node:child_process');
  const script = `
    const {postgresOptionsFromEnv} = require('./packages/request-store');
    postgresOptionsFromEnv({DATABASE_URL: 'postgres://user:pass@db/demo'});
    const loaded = Object.keys(require.cache)
        .filter((file) => file.includes('@aws-sdk'));
    process.stdout.write(JSON.stringify(loaded));`;
  const run = spawnSync(process.execPath, ['-e', script],
      {cwd: require('node:path').join(__dirname, '..'), encoding: 'utf8'});
  assert.equal(run.status, 0, run.stderr);
  assert.deepEqual(JSON.parse(run.stdout), []);
});
