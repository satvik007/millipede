import { Provider } from '@/components/provider';
import type { Metadata } from 'next';
import './global.css';

// Absolute base for metadata URLs (og:image, twitter:image). Without this,
// static export resolves them against http://localhost:3000, which the
// internal link check (`check:links`) intentionally does not skip.
// The default matches the planned GitHub Pages deployment; override with
// NEXT_PUBLIC_SITE_URL if the site moves.
export const metadata: Metadata = {
  metadataBase: new URL(
    process.env.NEXT_PUBLIC_SITE_URL ??
      `https://satvik007.github.io${process.env.NEXT_PUBLIC_BASE_PATH ?? ''}`,
  ),
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}
