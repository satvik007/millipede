import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import http from 'node:http';
import readline from 'node:readline';
import test from 'node:test';
import { add64, seaHash, toBigInt } from './sea_hash.mjs';

test('crawls, validates, and emits the Rust Sample schema', async (t) => {
  const pages = new Map([
    ['/nonce/p/0', Buffer.from('<html><body><a href="/nonce/p/1">next</a></body></html>')],
    ['/nonce/p/1', Buffer.from('<html><body>leaf</body></html>')],
  ]);
  const server = http.createServer((request, response) => {
    const body = pages.get(request.url);
    if (!body) {
      response.writeHead(404).end();
      return;
    }
    response.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'content-length': body.length,
    });
    response.end(body);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => server.close());

  let checksum = { lo: 0, hi: 0 };
  let decodedBytes = 0;
  for (const body of pages.values()) {
    checksum = add64(checksum, seaHash(body));
    decodedBytes += body.length;
  }
  const port = server.address().port;
  const child = spawn(process.execPath, [
    new URL('./runner.mjs', import.meta.url).pathname,
    '--scenario', 'tree',
    '--url', `http://127.0.0.1:${port}/nonce/p/0`,
    '--concurrency', '2',
    '--nonce', 'nonce',
    '--json',
  ], { stdio: ['pipe', 'pipe', 'inherit'] });
  t.after(() => child.kill());
  const lines = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  const iterator = lines[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value, 'ready');
  child.stdin.write(`${JSON.stringify({
    expected: {
      pages: pages.size,
      decoded_bytes: decodedBytes,
      checksum: toBigInt(checksum).toString(),
      records: null,
      digest: null,
    },
    entry_urls: [],
  })}\ngo\n`);
  const sample = JSON.parse((await iterator.next()).value);
  assert.equal(sample.engine, 'crawlee');
  assert.equal(sample.pages, 2);
  assert.equal(sample.bytes_decoded, decodedBytes);
  assert.equal(sample.valid, true, sample.validation_errors.join('; '));
  assert.deepEqual(sample.validation_errors, []);
  assert.equal((await new Promise((resolve) => child.once('exit', resolve))), 0);
});
