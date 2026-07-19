// SeaHash 4.1-compatible hashing using pairs of unsigned 32-bit words.
// Keeping the hot loop in Number/Math.imul arithmetic is substantially faster
// than applying JavaScript BigInt operations to every eight-byte block.

const P_LO = 0xa4d94a4f;
const P_HI = 0x6eed0e9d;

function multiplyHigh32(a, b) {
  const a0 = a & 0xffff;
  const a1 = a >>> 16;
  const b0 = b & 0xffff;
  const b1 = b >>> 16;
  const w0 = a0 * b0;
  const t = a1 * b0 + (w0 >>> 16);
  let w1 = t & 0xffff;
  const w2 = t >>> 16;
  w1 += a0 * b1;
  return (a1 * b1 + w2 + (w1 >>> 16)) >>> 0;
}

function multiplyByDiffuseConstant(lo, hi, out) {
  out[0] = Math.imul(lo, P_LO) >>> 0;
  out[1] = (
    multiplyHigh32(lo, P_LO)
    + Math.imul(hi, P_LO)
    + Math.imul(lo, P_HI)
  ) >>> 0;
}

function diffuse(lo, hi, out) {
  multiplyByDiffuseConstant(lo, hi, out);
  lo = out[0];
  hi = out[1];
  // Rust: x ^= (x >> 32) >> (x >> 60). The shifted value always fits in
  // the low word, and the dynamic shift is the top nibble of x.
  lo = (lo ^ (hi >>> (hi >>> 28))) >>> 0;
  multiplyByDiffuseConstant(lo, hi, out);
}

export function seaHash(bytes) {
  let aLo = 0x9b0d677c; let aHi = 0x16f11fe8;
  let bLo = 0xd8e6c86c; let bHi = 0xb480a793;
  let cLo = 0xf078ebc9; let cHi = 0x6fe2e5aa;
  let dLo = 0xc5259381; let dHi = 0x14f994a4;
  const mixed = new Uint32Array(2);

  let offset = 0;
  for (; offset + 8 <= bytes.length; offset += 8) {
    const nLo = bytes.readUInt32LE(offset);
    const nHi = bytes.readUInt32LE(offset + 4);
    diffuse((aLo ^ nLo) >>> 0, (aHi ^ nHi) >>> 0, mixed);
    aLo = bLo; aHi = bHi;
    bLo = cLo; bHi = cHi;
    cLo = dLo; cHi = dHi;
    dLo = mixed[0]; dHi = mixed[1];
  }

  if (offset < bytes.length) {
    let nLo = 0;
    let nHi = 0;
    const remaining = bytes.length - offset;
    for (let i = 0; i < Math.min(4, remaining); i += 1) {
      nLo = (nLo | (bytes[offset + i] << (i * 8))) >>> 0;
    }
    for (let i = 4; i < remaining; i += 1) {
      nHi = (nHi | (bytes[offset + i] << ((i - 4) * 8))) >>> 0;
    }
    diffuse((aLo ^ nLo) >>> 0, (aHi ^ nHi) >>> 0, mixed);
    aLo = bLo; aHi = bHi;
    bLo = cLo; bHi = cHi;
    cLo = dLo; cHi = dHi;
    dLo = mixed[0]; dHi = mixed[1];
  }

  const lengthLo = bytes.length >>> 0;
  const lengthHi = Math.floor(bytes.length / 0x1_0000_0000) >>> 0;
  diffuse(
    (aLo ^ bLo ^ cLo ^ dLo ^ lengthLo) >>> 0,
    (aHi ^ bHi ^ cHi ^ dHi ^ lengthHi) >>> 0,
    mixed,
  );
  return { lo: mixed[0], hi: mixed[1] };
}

export function add64(left, right) {
  const lo = (left.lo + right.lo) >>> 0;
  const carry = lo < left.lo ? 1 : 0;
  return { lo, hi: (left.hi + right.hi + carry) >>> 0 };
}

export function xor64(left, right) {
  return { lo: (left.lo ^ right.lo) >>> 0, hi: (left.hi ^ right.hi) >>> 0 };
}

export function rotateLeft64(value, amount) {
  const n = amount & 63;
  if (n === 0) return { ...value };
  if (n === 32) return { lo: value.hi, hi: value.lo };
  if (n < 32) {
    return {
      lo: ((value.lo << n) | (value.hi >>> (32 - n))) >>> 0,
      hi: ((value.hi << n) | (value.lo >>> (32 - n))) >>> 0,
    };
  }
  const m = n - 32;
  return {
    lo: ((value.hi << m) | (value.lo >>> (32 - m))) >>> 0,
    hi: ((value.lo << m) | (value.hi >>> (32 - m))) >>> 0,
  };
}

export function fromDecimal(value) {
  const n = BigInt(value);
  return {
    lo: Number(n & 0xffff_ffffn) >>> 0,
    hi: Number((n >> 32n) & 0xffff_ffffn) >>> 0,
  };
}

export function toBigInt(value) {
  return (BigInt(value.hi) << 32n) | BigInt(value.lo);
}
