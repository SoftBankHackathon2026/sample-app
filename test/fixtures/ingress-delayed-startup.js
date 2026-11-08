// Hold startup after the Redis lease is acquired, before HTTP listeners exist.
// Used only through NODE_OPTIONS in the isolated supervisor regression test.
const fs = require('node:fs/promises');
const {setTimeout: delay} = require('node:timers/promises');
const service = require('../../src/durable-ingress/service');
const createIngress = service.createIngress;
service.createIngress = async (...args) => {
  const app = await createIngress(...args);
  await fs.writeFile(process.env.DEMO_TEST_START_MARKER, 'ready');
  while (true) {
    try {
      await fs.access(process.env.DEMO_TEST_RELEASE_MARKER);
      break;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    await delay(20);
  }
  return app;
};
