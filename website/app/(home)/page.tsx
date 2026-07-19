import Link from 'next/link';

const cards = [
  {
    href: '/docs/quick-start',
    title: 'Quick start',
    description: 'Pick a crawler — HTTP, HTML, or browser — and get a crawl running in minutes.',
  },
  {
    href: '/docs/introduction',
    title: 'Introduction',
    description: 'A step-by-step course: build your first crawler, follow links, scrape and save data.',
  },
  {
    href: '/docs/examples',
    title: 'Examples',
    description: 'Complete, compiling programs straight from the repository, one page per example.',
  },
];

export default function HomePage() {
  return (
    <div className="flex flex-col items-center justify-center text-center flex-1 gap-6 px-4 py-16">
      <h1 className="text-4xl font-bold tracking-tight">Millipede</h1>
      <p className="text-lg text-fd-muted-foreground max-w-xl">
        Rust web crawling, Crawlee-shaped. An idiomatic library for building reliable HTTP, HTML,
        and browser crawlers.
      </p>
      <div className="flex flex-col items-center gap-2">
        <code className="rounded-lg border bg-fd-secondary px-4 py-2 font-mono text-sm">
          cargo add millipede --git https://github.com/satvik007/millipede
        </code>
        <p className="text-xs text-fd-muted-foreground">
          Once 0.1.0 lands on crates.io, this becomes plain{' '}
          <code className="font-mono">cargo add millipede</code>.
        </p>
      </div>
      <div className="grid gap-4 sm:grid-cols-3 max-w-3xl w-full mt-4">
        {cards.map((card) => (
          <Link
            key={card.href}
            href={card.href}
            className="rounded-lg border bg-fd-card p-4 text-left transition-colors hover:bg-fd-accent"
          >
            <h2 className="font-semibold mb-1">{card.title}</h2>
            <p className="text-sm text-fd-muted-foreground">{card.description}</p>
          </Link>
        ))}
      </div>
    </div>
  );
}
