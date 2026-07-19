#!/usr/bin/env node

import readline from 'node:readline';
import process from 'node:process';
import JSONBigFactory from 'json-bigint';
import {
  CheerioCrawler,
  Configuration,
  LogLevel,
  MemoryStorage,
  log,
} from 'crawlee';
import {
  add64,
  fromDecimal,
  rotateLeft64,
  seaHash,
  toBigInt,
  xor64,
} from './sea_hash.mjs';

const JSONBig = JSONBigFactory({ storeAsString: true });
const ZERO64 = Object.freeze({ lo: 0, hi: 0 });
const KNOWN_SCENARIOS = new Set([
  'tree', 'wide', 'mesh', 'latency', 'payload', 'redirects', 'compressed',
  'books', 'hn',
]);

function parseArgs(argv) {
  const args = argv[0] === 'run' ? argv.slice(1) : argv;
  const values = {};
  const toleratedFlags = new Set(['json']);
  const toleratedValues = new Set(['nonce', 'depth', 'runtime-workers']);
  for (let i = 0; i < args.length; i += 1) {
    const token = args[i];
    if (!token.startsWith('--')) throw new Error(`unexpected argument: ${token}`);
    const [rawName, inline] = token.slice(2).split('=', 2);
    if (toleratedFlags.has(rawName)) continue;
    if (!['scenario', 'url', 'concurrency', ...toleratedValues].includes(rawName)) {
      throw new Error(`unknown option: --${rawName}`);
    }
    const value = inline ?? args[++i];
    if (value === undefined || value.startsWith('--')) {
      throw new Error(`missing value for --${rawName}`);
    }
    values[rawName] = value;
  }
  if (!KNOWN_SCENARIOS.has(values.scenario)) {
    throw new Error(`unknown or missing --scenario: ${values.scenario ?? '<missing>'}`);
  }
  if (!values.url) throw new Error('missing --url');
  // Validate before ready; constructing URL does not create an HTTP client.
  const root = new URL(values.url);
  if (root.protocol !== 'http:' && root.protocol !== 'https:') {
    throw new Error(`unsupported URL protocol: ${root.protocol}`);
  }
  const concurrency = Number(values.concurrency ?? '32');
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new Error('--concurrency must be a positive integer');
  }
  return { scenario: values.scenario, url: root.href, concurrency };
}

async function receiveTrialWire() {
  process.stdout.write('ready\n');
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  const iterator = rl[Symbol.asyncIterator]();
  const specLine = await iterator.next();
  if (specLine.done) throw new Error('stdin closed before TrialWire');
  const wire = JSONBig.parse(specLine.value);
  const goLine = await iterator.next();
  rl.close();
  if (goLine.done || goLine.value.trim() !== 'go') {
    throw new Error(`expected go, received ${goLine.done ? '<EOF>' : JSON.stringify(goLine.value)}`);
  }
  if (!wire?.expected || !Array.isArray(wire.entry_urls)) {
    throw new Error('invalid TrialWire payload');
  }
  return wire;
}

function usage() {
  const value = process.resourceUsage();
  return {
    // Node normalizes maxRSS to KiB on all supported platforms.
    maxRssBytes: value.maxRSS * 1024,
    cpuUserMs: Math.floor(value.userCPUTime / 1000),
    cpuSysMs: Math.floor(value.systemCPUTime / 1000),
  };
}

function makeAccumulator() {
  return {
    pages: 0,
    bytesDecoded: 0,
    checksum: { ...ZERO64 },
    records: 0,
    digestSum: { ...ZERO64 },
    digestXor: { ...ZERO64 },
    errors: [],
  };
}

function foldHash(acc, hash, countRecord = true) {
  if (countRecord) acc.records += 1;
  acc.digestSum = add64(acc.digestSum, hash);
  acc.digestXor = xor64(acc.digestXor, hash);
}

function foldRecord(acc, record) {
  foldHash(acc, seaHash(Buffer.from(record, 'utf8')));
}

function extractBooks($, acc) {
  const titleNode = $('h1.title').first();
  if (titleNode.length === 0) return;
  const priceNode = $('p.price').first();
  if (priceNode.length === 0) return;
  const title = titleNode.text();
  const match = /^Book ([0-9]+)$/.exec(title);
  if (!match) return;
  foldRecord(acc, `${match[1]}\x1f${title}\x1f${priceNode.text()}`);
}

function extractHn($, acc) {
  if ($('div.item-page').first().length > 0) {
    const title = $('span.titleline > a').first().text();
    const score = $('span.score').first().text();
    foldRecord(acc, `story\x1f${title}\x1f${score}`);
    $('div.comment').each((_index, element) => {
      foldRecord(acc, `comment\x1f${$(element).text()}`);
    });
    return;
  }
  $('tr.athing span.titleline > a').each((_index, element) => {
    // Front-page titles contribute to sum/xor, but intentionally not records.
    foldHash(acc, seaHash(Buffer.from(`front\x1f${$(element).text()}`, 'utf8')), false);
  });
}

function validate(expected, acc) {
  const errors = [...acc.errors];
  const expectedPages = Number(expected.pages);
  const expectedBytes = Number(expected.decoded_bytes);
  if (acc.pages !== expectedPages) errors.push(`pages ${acc.pages} != expected ${expectedPages}`);
  if (acc.bytesDecoded !== expectedBytes) {
    errors.push(`decoded bytes ${acc.bytesDecoded} != expected ${expectedBytes}`);
  }
  const expectedChecksum = fromDecimal(expected.checksum);
  if (toBigInt(acc.checksum) !== toBigInt(expectedChecksum)) {
    errors.push(`checksum 0x${toBigInt(acc.checksum).toString(16)} != expected 0x${toBigInt(expectedChecksum).toString(16)}`);
  }
  if (expected.records !== null && expected.records !== undefined) {
    const expectedRecords = Number(expected.records);
    if (acc.records !== expectedRecords) {
      errors.push(`records ${acc.records} != expected ${expectedRecords}`);
    }
  }
  if (expected.digest !== null && expected.digest !== undefined) {
    const digest = add64(acc.digestSum, rotateLeft64(acc.digestXor, 17));
    const expectedDigest = fromDecimal(expected.digest);
    if (toBigInt(digest) !== toBigInt(expectedDigest)) {
      errors.push(`digest 0x${toBigInt(digest).toString(16)} != expected 0x${toBigInt(expectedDigest).toString(16)}`);
    }
  }
  return errors;
}

async function crawl(args, wire, acc) {
  const expectedPages = Number(wire.expected.pages);
  const rootHostname = new URL(args.url).hostname;
  const config = new Configuration({
    storageClient: new MemoryStorage({ persistStorage: false }),
    persistStorage: false,
    purgeOnStart: true,
  });
  const crawler = new CheerioCrawler({
    minConcurrency: args.concurrency,
    maxConcurrency: args.concurrency,
    maxRequestRetries: 0,
    maxRequestsPerCrawl: expectedPages + 16,
    navigationTimeoutSecs: 15,
    requestHandlerTimeoutSecs: 30,
    sameDomainDelaySecs: 0,
    useSessionPool: false,
    retryOnBlocked: false,
    respectRobotsTxtFile: false,
    preNavigationHooks: [(_context, requestOptions) => {
      requestOptions.maxRedirects = 7;
      requestOptions.headers ??= {};
      requestOptions.headers['user-agent'] = 'millipede-bench/1.0';
      requestOptions.hooks ??= {};
      requestOptions.hooks.beforeRedirect ??= [];
      requestOptions.hooks.beforeRedirect.push((redirectOptions) => {
        if (redirectOptions.url.hostname !== rootHostname) {
          throw new Error(`redirect left root hostname: ${redirectOptions.url.href}`);
        }
      });
    }],
    async requestHandler({ $, body, enqueueLinks }) {
      const decoded = Buffer.isBuffer(body) ? body : Buffer.from(body, 'utf8');
      acc.pages += 1;
      acc.bytesDecoded += decoded.length;
      acc.checksum = add64(acc.checksum, seaHash(decoded));
      if (args.scenario === 'books') extractBooks($, acc);
      if (args.scenario === 'hn') extractHn($, acc);
      await enqueueLinks({ strategy: 'same-hostname' });
    },
    failedRequestHandler({ request }, error) {
      acc.errors.push(`request failed: ${request.url}: ${error?.message ?? String(error)}`);
    },
  }, config);
  await crawler.run([args.url]);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  // Crawlee diagnostics must not add stdout lines to the wire protocol.
  log.setLevel(LogLevel.OFF);
  const wire = await receiveTrialWire();
  const readyUsage = usage();
  const acc = makeAccumulator();
  const start = process.hrtime.bigint();
  try {
    await crawl(args, wire, acc);
  } catch (error) {
    acc.errors.push(`engine error: ${error?.stack ?? String(error)}`);
  }
  const elapsedNs = process.hrtime.bigint() - start;
  const finalUsage = usage();
  const validationErrors = validate(wire.expected, acc);
  const elapsedSeconds = Number(elapsedNs) / 1e9;
  const sample = {
    scenario: args.scenario,
    engine: 'crawlee',
    pages: acc.pages,
    wall_ms: Number(elapsedNs / 1_000_000n),
    pages_per_sec: elapsedSeconds > 0 ? Number(wire.expected.pages) / elapsedSeconds : 0,
    bytes_decoded: acc.bytesDecoded,
    // The Rust orchestrator replaces this placeholder with server metrics.
    bytes_on_wire: acc.bytesDecoded,
    max_rss_bytes: finalUsage.maxRssBytes,
    ready_rss_bytes: readyUsage.maxRssBytes,
    cpu_user_ms: Math.max(0, finalUsage.cpuUserMs - readyUsage.cpuUserMs),
    cpu_sys_ms: Math.max(0, finalUsage.cpuSysMs - readyUsage.cpuSysMs),
    valid: validationErrors.length === 0,
    validation_errors: validationErrors,
  };
  process.stdout.write(`${JSON.stringify(sample)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error?.stack ?? String(error)}\n`);
  process.exitCode = 1;
});
