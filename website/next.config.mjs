import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

/** @type {import('next').NextConfig} */
const config = {
  output: 'export',
  reactStrictMode: true,
  trailingSlash: true,
  images: { unoptimized: true },
  // Empty locally and in CI; the deferred GitHub Pages deploy sets
  // NEXT_PUBLIC_BASE_PATH=/millipede.
  basePath,
  assetPrefix: basePath,
};

export default withMDX(config);
