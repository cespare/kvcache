package main

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cespare/kvcache/internal/github.com/garyburd/redigo/redis"
)

// A small integration test.
func TestServer(t *testing.T) {
	tempdir, err := ioutil.TempDir(".", "testdata-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempdir)
	dir := filepath.Join(tempdir, "db")

	server, err := NewServer(dir, "", 1e6, time.Hour, "localhost:8125", false)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	go func() {
		if err := server.loop(l); err != nil {
			t.Fatal(err)
		}
	}()
	defer func() {
		if err := server.Stop(); err != nil {
			t.Fatal(err)
		}
	}()

	c, err := redis.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		cmd  string
		args []interface{}
		want string // "nil" means nil response
	}{
		{"GET", []interface{}{"foo"}, "nil"},
		{"SET", []interface{}{"foo", "bar", "NX"}, "OK"},
		{"GET", []interface{}{"foo"}, "bar"},
		{"SET", []interface{}{"foo", "baz", "NX"}, "nil"},
		{"GET", []interface{}{"foo"}, "bar"},
	} {
		reply, err := redis.String(c.Do(testCase.cmd, testCase.args...))
		switch err {
		case nil:
			if testCase.want != reply {
				if testCase.want == "nil" {
					t.Fatalf("want: nil; got: %s", reply)
				}
				t.Fatalf("want: %s; got: %s", testCase.want, reply)
			}
		case redis.ErrNil:
			if testCase.want != "nil" {
				t.Fatalf("want: %s; got: nil", testCase.want)
			}
		default:
			t.Fatal(err)
		}
	}
}
