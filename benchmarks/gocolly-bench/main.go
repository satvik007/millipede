// gocolly-bench is a child runner for benchmarks/spider-bench's ready/spec/go
// protocol. The synthetic site and expected values remain owned by the Rust
// parent so rendering does not contaminate this process's resource figures.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
	"golang.org/x/sys/unix"
)

type expected struct {
	Pages        uint64  `json:"pages"`
	DecodedBytes uint64  `json:"decoded_bytes"`
	Checksum     uint64  `json:"checksum"`
	Records      *uint64 `json:"records"`
	Digest       *uint64 `json:"digest"`
}

type trialWire struct {
	Expected  expected `json:"expected"`
	EntryURLs []string `json:"entry_urls"`
}

// sample deliberately mirrors benchmarks/spider-bench/src/measure.rs.
type sample struct {
	Scenario         string   `json:"scenario"`
	Engine           string   `json:"engine"`
	Pages            uint64   `json:"pages"`
	WallMS           uint64   `json:"wall_ms"`
	PagesPerSec      float64  `json:"pages_per_sec"`
	BytesDecoded     uint64   `json:"bytes_decoded"`
	BytesOnWire      uint64   `json:"bytes_on_wire"`
	MaxRSSBytes      uint64   `json:"max_rss_bytes"`
	ReadyRSSBytes    uint64   `json:"ready_rss_bytes"`
	CPUUserMS        uint64   `json:"cpu_user_ms"`
	CPUSysMS         uint64   `json:"cpu_sys_ms"`
	Valid            bool     `json:"valid"`
	ValidationErrors []string `json:"validation_errors"`
}

type digest struct {
	count uint64
	sum   uint64
	xor   uint64
}

func (d *digest) record(record []byte) {
	h := seaHash(record)
	d.count++
	d.sum += h // Go uint64 arithmetic wraps, matching Rust wrapping_add.
	d.xor ^= h
}

func (d digest) value() uint64 { return d.sum + (d.xor<<17 | d.xor>>(64-17)) }

type result struct {
	mu       sync.Mutex
	pages    uint64
	bytes    uint64
	checksum uint64
	digest   digest
	errors   []string
}

func (r *result) recordBody(body []byte) {
	h := seaHash(body)
	r.mu.Lock()
	r.pages++
	r.bytes += uint64(len(body))
	r.checksum += h
	r.mu.Unlock()
}

func (r *result) mergeDigest(local digest) {
	r.mu.Lock()
	r.digest.count += local.count
	r.digest.sum += local.sum
	r.digest.xor ^= local.xor
	r.mu.Unlock()
}

func (r *result) addError(err string) {
	r.mu.Lock()
	r.errors = append(r.errors, err)
	r.mu.Unlock()
}

func main() {
	args := os.Args[1:]
	// Tolerate the Rust child's `run` subcommand shape as well as direct use.
	if len(args) != 0 && args[0] == "run" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("gocolly-bench", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario name")
	rootURL := fs.String("url", "", "root URL")
	concurrency := fs.Int("concurrency", 32, "fixed request concurrency")
	_ = fs.String("nonce", "", "accepted for harness compatibility")
	_ = fs.String("depth", "", "accepted for harness compatibility")
	_ = fs.Bool("json", false, "accepted for harness compatibility")
	runtimeWorkers := fs.Int("runtime-workers", 0, "Go scheduler worker cap")
	if err := fs.Parse(args); err != nil {
		fatal(err)
	}
	if *scenario == "" || *rootURL == "" || *concurrency < 1 {
		fatal(errors.New("--scenario, --url, and --concurrency >= 1 are required"))
	}
	if *scenario != "books" && *scenario != "hn" && !isRawScenario(*scenario) {
		fatal(fmt.Errorf("unknown scenario %q", *scenario))
	}
	if *runtimeWorkers > 0 {
		runtime.GOMAXPROCS(*runtimeWorkers)
	}

	wire, err := handshake()
	if err != nil {
		fatal(err)
	}
	readyUsage, err := selfUsage()
	if err != nil {
		fatal(err)
	}

	started := time.Now()
	out := crawl(*scenario, *rootURL, *concurrency)
	wall := time.Since(started)
	usage, usageErr := selfUsage()
	if usageErr != nil {
		out.addError("getrusage: " + usageErr.Error())
	}

	out.mu.Lock()
	validate(wire.Expected, out)
	wallMS := uint64(wall / time.Millisecond)
	rate := 0.0
	if wall > 0 {
		rate = float64(wire.Expected.Pages) / wall.Seconds()
	}
	s := sample{
		Scenario: *scenario, Engine: "gocolly", Pages: out.pages,
		WallMS: wallMS, PagesPerSec: rate, BytesDecoded: out.bytes,
		// The Rust orchestrator replaces this identity placeholder with its
		// authoritative server-side wire-byte counter.
		BytesOnWire: out.bytes, MaxRSSBytes: usage.maxRSSBytes,
		ReadyRSSBytes: readyUsage.maxRSSBytes, CPUUserMS: usage.userMS - readyUsage.userMS,
		CPUSysMS: usage.sysMS - readyUsage.sysMS, Valid: len(out.errors) == 0,
		ValidationErrors: append([]string{}, out.errors...),
	}
	out.mu.Unlock()
	if err := json.NewEncoder(os.Stdout).Encode(s); err != nil {
		fatal(err)
	}
}

func isRawScenario(name string) bool {
	switch name {
	case "tree", "wide", "mesh", "latency", "payload", "redirects", "compressed":
		return true
	default:
		return false
	}
}

func handshake() (trialWire, error) {
	// Absolutely no HTTP client or collector has been constructed yet.
	fmt.Println("ready")
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return trialWire{}, fmt.Errorf("read TrialWire: %w", err)
	}
	var wire trialWire
	if err := json.Unmarshal(line, &wire); err != nil {
		return trialWire{}, fmt.Errorf("decode TrialWire: %w", err)
	}
	line, err = r.ReadBytes('\n')
	if err != nil {
		return trialWire{}, fmt.Errorf("read go: %w", err)
	}
	if strings.TrimSpace(string(line)) != "go" {
		return trialWire{}, fmt.Errorf("expected go, got %q", strings.TrimSpace(string(line)))
	}
	return wire, nil
}

func crawl(scenario, root string, parallelism int) *result {
	out := &result{}
	u, err := url.Parse(root)
	if err != nil || u.Hostname() == "" {
		out.addError("invalid root URL: " + root)
		return out
	}

	c := colly.NewCollector(
		colly.Async(true),
		colly.MaxDepth(0),                  // unlimited
		colly.AllowedDomains(u.Hostname()), // same-hostname only
		colly.IgnoreRobotsTxt(),
		colly.UserAgent("millipede-bench/1.0"),
	)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns = parallelism
	transport.MaxIdleConnsPerHost = parallelism
	transport.MaxConnsPerHost = parallelism
	c.WithTransport(transport)
	c.SetRequestTimeout(15 * time.Second)
	c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
		if len(via) >= 7 {
			return errors.New("stopped after 7 redirects")
		}
		if req.URL.Hostname() != u.Hostname() {
			return errors.New("redirect left root hostname")
		}
		return nil
	})
	if err := c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: parallelism, Delay: 0}); err != nil {
		out.addError("configure limit: " + err.Error())
		return out
	}
	// Colly's built-in async visited check is a non-atomic check/store pair, so
	// dense frontiers can admit the same URL concurrently. Keep the benchmark's
	// exact-once contract with an atomic admission set, as production Colly
	// crawlers must do when duplicate fetches are unacceptable.
	var admitted sync.Map
	rootKey := *u
	rootKey.Fragment = ""
	admitted.Store(rootKey.String(), struct{}{})

	c.OnResponse(func(resp *colly.Response) { out.recordBody(resp.Body) })
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		absolute := e.Request.AbsoluteURL(e.Attr("href"))
		link, err := url.Parse(absolute)
		if err != nil || link.Hostname() != u.Hostname() {
			return
		}
		link.Fragment = ""
		absolute = link.String()
		if _, loaded := admitted.LoadOrStore(absolute, struct{}{}); loaded {
			return
		}
		if err := e.Request.Visit(absolute); err != nil {
			var already *colly.AlreadyVisitedError
			if !errors.As(err, &already) && !errors.Is(err, colly.ErrForbiddenDomain) {
				out.addError("visit " + absolute + ": " + err.Error())
			}
		}
	})
	if scenario == "books" {
		c.OnHTML("html", func(e *colly.HTMLElement) {
			var local digest
			extractBooks(e.DOM, &local)
			out.mergeDigest(local)
		})
	} else if scenario == "hn" {
		c.OnHTML("html", func(e *colly.HTMLElement) {
			var local digest
			extractHN(e.DOM, &local)
			out.mergeDigest(local)
		})
	}
	c.OnError(func(resp *colly.Response, err error) {
		out.addError("request " + resp.Request.URL.String() + ": " + err.Error())
	})

	if err := c.Visit(root); err != nil {
		out.addError("seed: " + err.Error())
	}
	c.Wait() // full callback and request drain is inside the timed region
	return out
}

func extractBooks(doc *goquery.Selection, d *digest) {
	title := doc.Find("h1.title").First().Text()
	if title == "" {
		return
	}
	price := doc.Find("p.price").First().Text()
	if price == "" {
		return
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(title, "Book "), 10, 64)
	if err != nil || !strings.HasPrefix(title, "Book ") {
		return
	}
	d.record([]byte(fmt.Sprintf("%d\x1f%s\x1f%s", id, title, price)))
}

func extractHN(doc *goquery.Selection, d *digest) {
	if doc.Find("div.item-page").First().Length() != 0 {
		title := doc.Find("span.titleline > a").First().Text()
		score := doc.Find("span.score").First().Text()
		d.record([]byte("story\x1f" + title + "\x1f" + score))
		doc.Find("div.comment").Each(func(_ int, s *goquery.Selection) {
			d.record([]byte("comment\x1f" + s.Text()))
		})
		return
	}
	doc.Find("tr.athing span.titleline > a").Each(func(_ int, s *goquery.Selection) {
		h := seaHash([]byte("front\x1f" + s.Text()))
		d.sum += h
		d.xor ^= h
	})
}

func validate(w expected, got *result) {
	if got.pages != w.Pages {
		got.errors = append(got.errors, fmt.Sprintf("pages %d != expected %d", got.pages, w.Pages))
	}
	if got.bytes != w.DecodedBytes {
		got.errors = append(got.errors, fmt.Sprintf("decoded bytes %d != expected %d", got.bytes, w.DecodedBytes))
	}
	if got.checksum != w.Checksum {
		got.errors = append(got.errors, fmt.Sprintf("checksum %#x != expected %#x", got.checksum, w.Checksum))
	}
	if w.Records != nil && got.digest.count != *w.Records {
		got.errors = append(got.errors, fmt.Sprintf("records %d != expected %d", got.digest.count, *w.Records))
	}
	if w.Digest != nil && got.digest.value() != *w.Digest {
		got.errors = append(got.errors, fmt.Sprintf("digest %#x != expected %#x", got.digest.value(), *w.Digest))
	}
}

type usage struct{ maxRSSBytes, userMS, sysMS uint64 }

func selfUsage() (usage, error) {
	var r unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &r); err != nil {
		return usage{}, err
	}
	rss := uint64(r.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024 // Linux and the other supported Unix targets report KiB.
	}
	toMS := func(sec, usec int64) uint64 { return uint64(sec)*1000 + uint64(usec)/1000 }
	return usage{rss, toMS(r.Utime.Sec, int64(r.Utime.Usec)), toMS(r.Stime.Sec, int64(r.Stime.Usec))}, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "gocolly-bench:", err)
	os.Exit(2)
}

// seaHash implements the four independent lanes of Rust seahash 4.1.0. Full
// 32-byte groups are unrolled; the short tail uses the equivalent rotating
// reference transition. Keeping it local avoids cross-language hash drift.
func seaHash(buf []byte) uint64 {
	total := len(buf)
	a := uint64(0x16f11fe89b0d677c)
	b := uint64(0xb480a793d8e6c86c)
	c := uint64(0x6fe2e5aaf078ebc9)
	d := uint64(0x14f994a4c5259381)
	for len(buf) >= 32 {
		a = diffuse(a ^ binary.LittleEndian.Uint64(buf[0:8]))
		b = diffuse(b ^ binary.LittleEndian.Uint64(buf[8:16]))
		c = diffuse(c ^ binary.LittleEndian.Uint64(buf[16:24]))
		d = diffuse(d ^ binary.LittleEndian.Uint64(buf[24:32]))
		buf = buf[32:]
	}
	for len(buf) != 0 {
		n := len(buf)
		if n > 8 {
			n = 8
		}
		var block [8]byte
		copy(block[:], buf[:n])
		x := binary.LittleEndian.Uint64(block[:])
		a, b, c, d = b, c, d, diffuse(a^x)
		buf = buf[n:]
	}
	return diffuse(a ^ b ^ c ^ d ^ uint64(total))
}

func diffuse(x uint64) uint64 {
	x *= 0x6eed0e9da4d94a4f
	x ^= (x >> 32) >> (x >> 60)
	x *= 0x6eed0e9da4d94a4f
	return x
}
