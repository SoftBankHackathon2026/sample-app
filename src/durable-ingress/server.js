const {createClient} = require('redis');
const {createIngress} = require('./service');

async function main() {
  const targets = JSON.parse(process.env.INGRESS_TARGETS || '{}');
  for (const target of Object.values(targets)) {
    if (target.token_env) {
      target.token = process.env[target.token_env];
      if (!target.token) throw new Error('Missing target token');
    }
  }
  const redis = createClient({url: process.env.REDIS_URL,
    disableOfflineQueue: true, socket: {connectTimeout: 3000}});
  redis.on('error', () => {});
  await redis.connect();
  // Control API: loopback by default. Set INGRESS_CONTROL_HOST=0.0.0.0 only together with a
  // NetworkPolicy that limits 9092 to the cutover runner, and preferably INGRESS_CONTROL_TOKEN.
  const controlHost = process.env.INGRESS_CONTROL_HOST || '127.0.0.1';
  const app = await createIngress({redis, targets,
    concurrency: Number(process.env.INGRESS_CONCURRENCY || 10),
    controlToken: process.env.INGRESS_CONTROL_TOKEN});
  const api = app.api.listen(Number(process.env.INGRESS_API_PORT || 8080), '0.0.0.0');
  const control = app.control.listen(Number(process.env.INGRESS_CONTROL_PORT || 9092), controlHost);
  process.once('SIGTERM', async () => {
    api.close(); control.close(); await app.close(); await redis.quit();
  });
}
main().catch(() => { console.error('INGRESS_START_FAILED'); process.exit(1); });
