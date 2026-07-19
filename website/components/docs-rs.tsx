import type { ReactNode } from 'react';

export interface DocsRsProps {
  /** Crate name on docs.rs, e.g. "millipede-http". Defaults to the umbrella crate. */
  crate?: string;
  /** Item path within the crate docs, e.g. "struct.HttpCrawler.html" or "router/index.html". */
  item?: string;
  children: ReactNode;
}

/**
 * External link to docs.rs API documentation.
 *
 * Usage in MDX: <DocsRs crate="millipede-http" item="struct.HttpCrawler.html">HttpCrawler</DocsRs>
 */
export function DocsRs({ crate = 'millipede', item, children }: DocsRsProps) {
  const href = `https://docs.rs/${crate}/latest/${crate.replaceAll('-', '_')}/${item ?? ''}`;

  return (
    <a href={href} target="_blank" rel="noreferrer">
      {children}
    </a>
  );
}
