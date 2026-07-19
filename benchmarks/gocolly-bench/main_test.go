package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

func document(t *testing.T, html string) *goquery.Selection {
	t.Helper()
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatal(err)
	}
	return doc.Selection
}

func TestSeaHashRustVector(t *testing.T) {
	if got, want := seaHash([]byte("to be or not to be")), uint64(1988685042348123509); got != want {
		t.Fatalf("seaHash = %d, want %d", got, want)
	}
}

func TestSeaHashUnrolledMatchesReferenceAcrossTails(t *testing.T) {
	buf := make([]byte, 257)
	for i := range buf {
		buf[i] = byte(i*31 + 7)
	}
	for n := range buf {
		if got, want := seaHash(buf[:n]), seaHashReference(buf[:n]); got != want {
			t.Fatalf("length %d: seaHash=%#x reference=%#x", n, got, want)
		}
	}
}

func seaHashReference(buf []byte) uint64 {
	total := len(buf)
	a := uint64(0x16f11fe89b0d677c)
	b := uint64(0xb480a793d8e6c86c)
	c := uint64(0x6fe2e5aaf078ebc9)
	d := uint64(0x14f994a4c5259381)
	for len(buf) != 0 {
		n := min(len(buf), 8)
		var x uint64
		for i := n - 1; i >= 0; i-- {
			x = x<<8 | uint64(buf[i])
		}
		a, b, c, d = b, c, d, diffuse(a^x)
		buf = buf[n:]
	}
	return diffuse(a ^ b ^ c ^ d ^ uint64(total))
}

func TestBooksExtraction(t *testing.T) {
	var got digest
	extractBooks(document(t, `<html><h1 class="title">Book 42</h1><p class="price">£9.99</p></html>`), &got)
	var want digest
	want.record([]byte("42\x1fBook 42\x1f£9.99"))
	if got != want {
		t.Fatalf("digest = %#v, want %#v", got, want)
	}
}

func TestHNExtraction(t *testing.T) {
	var got digest
	extractHN(document(t, `<html><div class="item-page"><span class="titleline"><a>T</a></span><span class="score">7 points</span><div class="comment">C1</div><div class="comment">C2</div></div></html>`), &got)
	var want digest
	want.record([]byte("story\x1fT\x1f7 points"))
	want.record([]byte("comment\x1fC1"))
	want.record([]byte("comment\x1fC2"))
	if got != want {
		t.Fatalf("digest = %#v, want %#v", got, want)
	}
}

func TestCrawlDiscoversSameHostAndIgnoresTrap(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/root":
			fmt.Fprintf(w, `<html><a href="/child">child</a><a href="http://localhost/offsite">trap</a></html>`)
		case "/child":
			fmt.Fprint(w, `<html>done</html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	got := crawl("tree", server.URL+"/root", 2)
	if got.pages != 2 || len(got.errors) != 0 {
		t.Fatalf("pages=%d errors=%v", got.pages, got.errors)
	}
}
