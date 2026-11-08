const express = require('express');
const {once} = require('node:events');
const {createClient} = require('redis');
const {createIngress, controlAuth} = require('./service');

function configuredTargets(name) {
  const targets = JSON.parse(process.env[name] || '{}');
  for (const target of Object.values(targets)) {
    if (target.token_env) {
      target.token = process.env[target.token_env];
      if (!target.token) throw new Error('MISSING_TARGET_TOKEN');
    }
  }
  return targets;
}

async function main() {
  const targets = configuredTargets('INGRESS_TARGETS');
  const forwardTargets = configuredTargets('INGRESS_FORWARD_TARGETS');
  const controlToken = process.env.INGRESS_CONTROL_TOKEN;
  const exposeControl = process.env.INGRESS_EXPOSE_CONTROL === 'true';
  if (exposeControl && !controlToken) throw new Error('CONTROL_AUTH_REQUIRED');
  const standby = process.env.INGRESS_STANDBY === 'true';
  const apiToken = process.env.INGRESS_API_TOKEN;
  if ((exposeControl || standby) && !apiToken) {
    throw new Error('API_AUTH_REQUIRED');
  }
  const redis = createClient({url: process.env.REDIS_URL,
    disableOfflineQueue: true, socket: {connectTimeout: 3000}});
  redis.on('error', () => {});
  await redis.connect();
  // 9092 remains private. Optional /control shares the TLS ingress's port,
  // with mandatory authentication and no token in the manifest or URL.
  const controlHost = process.env.INGRESS_CONTROL_HOST || '127.0.0.1';
  const app = await createIngress({redis, targets, controlToken, forwardTargets,
    initialTarget: process.env.INGRESS_INITIAL_TARGET || 'onprem',
    standby});
  const external = express();
  external.disable('x-powered-by');
  if (exposeControl) external.use('/control', app.control);
  external.use('/api', controlAuth(apiToken));
  external.use(app.api);
  const api = external.listen(Number(process.env.INGRESS_API_PORT || 8080),
      '0.0.0.0');
  const control = app.control.listen(
      Number(process.env.INGRESS_CONTROL_PORT || 9092), controlHost);
  await Promise.all([once(api, 'listening'), once(control, 'listening')]);
  return async () => {
    api.close();
    control.close();
    await app.close();
    await redis.quit();
  };
}
// Install shutdown handling before Redis initialization can acquire its lease.
// A stop during startup must finish initialization, then release that lease.
const initialized = main();
process.once('SIGTERM', () => {
  initialized.then((close) => close()).catch(() => {
    console.error('INGRESS_STOP_FAILED');
    process.exitCode = 1;
  });
});
initialized.catch(() => {
  console.error('INGRESS_START_FAILED');
  process.exit(1);
});
