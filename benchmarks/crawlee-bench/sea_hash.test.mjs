import assert from 'node:assert/strict';
import test from 'node:test';
import { add64, rotateLeft64, seaHash, toBigInt } from './sea_hash.mjs';

test('matches the seahash 4.1 reference vector', () => {
  const hash = seaHash(Buffer.from('to be or not to be'));
  assert.equal(toBigInt(hash), 1_988_685_042_348_123_509n);
});

test('wraps 64-bit addition and rotates across word boundaries', () => {
  assert.equal(toBigInt(add64({ lo: 0xffff_ffff, hi: 0xffff_ffff }, { lo: 1, hi: 0 })), 0n);
  assert.equal(toBigInt(rotateLeft64({ lo: 1, hi: 2 }, 32)), 0x0000_0001_0000_0002n);
  assert.equal(toBigInt(rotateLeft64({ lo: 1, hi: 0 }, 63)), 0x8000_0000_0000_0000n);
});
