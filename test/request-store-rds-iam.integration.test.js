const {test} = require('node:test');
const assert = require('node:assert/strict');
const {
  createPostgresRequestStore, postgresOptionsFromEnv,
} = require('../packages/request-store');

const connectionString = process.env.TEST_DATABASE_URL;

test('real PostgreSQL: RDS IAM mode logs in only through the token callback',
    {skip: !connectionString}, async (t) => {
      // The test database has no IAM, so the signer stub hands out its
      // password as the token. The URL itself carries no password.
      const url = new URL(connectionString);
      const secret = decodeURIComponent(url.password);
      url.password = '';
      let signed = 0;
      const options = postgresOptionsFromEnv({
        DATABASE_URL: url.toString(), DB_AUTH_MODE: 'rds-iam',
        AWS_REGION: 'test-region', PGSSLMODE: 'verify-full', DB_POOL_MAX: '2',
      }, {signerFactory: () => ({getAuthToken: async () => {
        signed++;
        return secret;
      }})});
      // The disposable test server has no TLS; RDS keeps verified TLS.
      const store = createPostgresRequestStore({...options, ssl: false});
      t.after(() => store.close());
      const first = await store.checkReady();
      assert.equal(first.name, url.pathname.slice(1));
      assert.equal(signed, 1);
      await Promise.all([store.status(), store.status()]);
      assert.equal(signed, 2, 'a second pooled connection signs again');
    });
