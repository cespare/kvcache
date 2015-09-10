package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cespare/kvcache/internal/github.com/cespare/gostc"
	"github.com/cespare/kvcache/internal/github.com/dustin/go-humanize"
)

var version = "no version set" // may be overridden at build time with -ldflags -X

type Server struct {
	addr            string
	db              *DB
	statsd          *gostc.Client
	quitStatUpdates chan struct{}
}

func NewServer(dir, addr string, chunkSize uint64, expiry time.Duration, statsdAddr string, removeCorrupt bool) (*Server, error) {
	statsd, err := gostc.NewClient(statsdAddr)
	if err != nil {
		return nil, err
	}
	db, removedChunks, err := OpenDB(chunkSize, expiry, dir, removeCorrupt)
	if err != nil {
		return nil, err
	}
	statsd.Count("kvcache.corrupt-removed-chunks", float64(removedChunks), 1.0)
	return &Server{
		addr:            addr,
		db:              db,
		statsd:          statsd,
		quitStatUpdates: make(chan struct{}),
	}, nil
}

type Request struct {
	Type RequestType
	Key  []byte
	Val  []byte
	Resp chan *Response
	Err  error // If there was an error reading the request, only this field is set.
}

type RequestType uint8

const (
	RequestSet RequestType = iota + 1
	RequestGet
	RequestPing
	RequestInfo
)

type RedisType uint8

const (
	RedisErr RedisType = iota + 1
	RedisString
	RedisBulk
)

type Response struct {
	Type RedisType
	Msg  []byte
}

func (s *Server) Stop() error {
	close(s.quitStatUpdates)
	s.statsd.Close()
	return s.db.Close()
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.statsd.Inc("kvcache.server-start")
	go s.statUpdates()
	return s.loop(l)
}

func (s *Server) statUpdates() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats := s.db.Info()
			s.statsd.Gauge("kvcache.db.rchunks", float64(stats.RChunks))
			s.statsd.Gauge("kvcache.db.total-rlog-size", float64(stats.TotalRLogSize))
			s.statsd.Gauge("kvcache.db.wlog-keys", float64(stats.WLogKeys))
			s.statsd.Gauge("kvcache.db.rlog-keys", float64(stats.RLogKeys))
			s.statsd.Gauge("kvcache.db.total-keys", float64(stats.TotalKeys))
		case <-s.quitStatUpdates:
			return
		}
	}
}

func (s *Server) loop(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				delay := 10 * time.Millisecond
				log.Printf("Accept error: %s; retrying in %s", e, delay)
				s.statsd.Inc("kvcache.errors.accept")
				time.Sleep(delay)
				continue
			}
			return err
		}
		go s.HandleConn(c)
	}
}

func when(ch chan *Response, pred bool) chan *Response {
	if pred {
		return ch
	}
	return nil
}

func head(q []*Response) *Response {
	if len(q) == 0 {
		return nil
	}
	return q[0]
}

func (s *Server) HandleConn(c net.Conn) {
	log.Printf("Client connected from %s", c.RemoteAddr())
	s.statsd.Inc("kvcache.client-connect")

	// readErr and writeErr are how the request reader and response writer goroutines can notify the other that
	// the client (or connection) broke/disconnected.
	// These signal chans are only closed.
	readErr := make(chan struct{})
	writeErr := make(chan struct{})

	// This request goroutine reads requests and sends them into this goroutine to be handled and buffered;
	// Responses are sent off to the response goroutine.
	// This is necessary for Redis pipelining to work.
	requests := make(chan *Request)
	responses := make(chan *Response)

	go s.readRequests(c, requests, readErr, writeErr)
	go s.writeResponses(c, responses, readErr, writeErr)

	var responseQueue []*Response

reqLoop:
	for {
		resp := &Response{Type: RedisString}
		select {
		case r := <-requests:
			if r.Err != nil {
				s.statsd.Inc("kvcache.errors.request")
				resp.Msg = []byte(r.Err.Error())
				resp.Type = RedisErr
			} else {
				s.statsd.Inc("kvcache.requests")
				switch r.Type {
				case RequestSet:
					s.statsd.Inc("kvcache.requests.set")
					start := time.Now()
					_, err := s.db.Put(r.Key, r.Val)
					switch err {
					case nil:
						resp.Msg = []byte("OK")
					case ErrKeyExist:
						resp.Type = RedisBulk // null
					default:
						if e, ok := err.(FatalDBError); ok {
							log.Println("Fatal DB error:", e)
							if err := s.Stop(); err != nil {
								log.Println("Error while shutting down:", err)
							}
							os.Exit(1)
						}
						resp = ResponseFromError(err)
					}
					s.statsd.Time("kvcache.requests.set", time.Since(start))
				case RequestGet:
					s.statsd.Inc("kvcache.requests.get")
					start := time.Now()
					v, _, err := s.db.Get(r.Key)
					switch err {
					case nil:
						resp.Type = RedisBulk
						resp.Msg = v
					case ErrKeyNotExist:
						resp.Type = RedisBulk // null
					default:
						resp = ResponseFromError(err)
					}
					s.statsd.Time("kvcache.requests.get", time.Since(start))
				case RequestPing:
					s.statsd.Inc("kvcache.requests.ping")
					resp.Msg = []byte("PONG")
				case RequestInfo:
					s.statsd.Inc("kvcache.requests.info")
					resp.Msg = []byte(s.db.Info().String())
				default:
					s.statsd.Inc("kvcache.errors.unexpected-request-type")
					panic("unexpected request type")
				}
			}
			responseQueue = append(responseQueue, resp)
		case when(responses, len(responseQueue) > 0) <- head(responseQueue):
			responseQueue = responseQueue[1:]
		case <-readErr:
			break reqLoop
		case <-writeErr:
			break reqLoop
		}
	}

	log.Printf("Client disconnected from %s", c.RemoteAddr())
	c.Close()
}

func (s *Server) readRequests(c net.Conn, requests chan<- *Request, readErr, writeErr chan struct{}) {
	br := bufio.NewReader(c)
	for {
		var r Request
		if err := r.Parse(br); err != nil {
			if _, ok := err.(net.Error); ok {
				close(readErr)
				return
			}
			r.Err = err
		}
		select {
		case requests <- &r:
		case <-writeErr:
			return
		}
	}
}

func (s *Server) writeResponses(c net.Conn, responses <-chan *Response, readErr, writeErr chan struct{}) {
	for {
		select {
		case resp := <-responses:
			if err := resp.Write(c); err != nil {
				close(writeErr)
				return
			}
		case <-readErr:
			return
		}
	}
}

func ResponseFromError(err error) *Response {
	return &Response{
		Type: RedisErr,
		Msg:  []byte(err.Error()),
	}
}

var (
	ErrMalformedRequest    = errors.New("malformed request")
	ErrUnrecognizedCommand = errors.New("unrecognized command")
	ErrWrongNumArgs        = errors.New("wrong number of arguments for command")
	ErrSetXXUnupported     = errors.New("the XX option to SET is not supported")
	ErrSetNXRequired       = errors.New("the NX option to SET is required")
)

func (r *Request) Parse(br *bufio.Reader) error {
	array, err := parseRedisArrayBulkString(br)
	if err != nil {
		return err
	}
	if len(array) == 0 {
		return ErrMalformedRequest
	}
	switch strings.ToUpper(array[0]) {
	case "SET":
		if len(array) < 3 {
			return ErrWrongNumArgs
		}
		r.Type = RequestSet
		r.Key = []byte(array[1])
		r.Val = []byte(array[2])
		var nx bool
		// Note permissive redis behavior:
		// https://github.com/antirez/redis/issues/2157
		for i := 3; i < len(array); i++ {
			param := strings.ToUpper(array[i])
			switch param {
			case "EX", "PX":
				if i+1 >= len(array) {
					return fmt.Errorf("expiry parameter %s provided without a value", param)
				}
				i++ // Skip the expiry value
			case "XX":
				return ErrSetXXUnupported
			case "NX":
				nx = true
			}
		}
		if !nx {
			return ErrSetNXRequired
		}
	case "GET":
		if len(array) != 2 {
			return ErrWrongNumArgs
		}
		r.Type = RequestGet
		r.Key = []byte(array[1])
	case "PING":
		if len(array) != 1 {
			return ErrWrongNumArgs
		}
		r.Type = RequestPing
	case "INFO":
		if len(array) != 1 {
			return ErrWrongNumArgs
		}
		r.Type = RequestInfo
	default:
		return ErrUnrecognizedCommand
	}
	return nil
}

func (r *Response) Write(w io.Writer) error {
	var msg []byte
	switch r.Type {
	case RedisErr:
		msg = append([]byte("-"), r.Msg...)
		msg = append(msg, "\r\n"...)
	case RedisString:
		msg = append([]byte("+"), r.Msg...)
		msg = append(msg, "\r\n"...)
	case RedisBulk:
		if r.Msg == nil {
			msg = []byte("$-1\r\n")
			break
		}
		msg = append([]byte{'$'}, strconv.Itoa(len(r.Msg))...)
		msg = append(msg, "\r\n"...)
		msg = append(msg, r.Msg...)
		msg = append(msg, "\r\n"...)
	default:
		panic("unexpected response type")
	}
	_, err := w.Write(msg)
	return err
}

func main() {
	var (
		addr          = flag.String("addr", "localhost:5533", "Listen addr")
		dir           = flag.String("dir", "db", "DB directory")
		chunkSize     = flag.String("chunksize", "100MB", "Max size for chunks")
		expiry        = flag.Duration("expiry", time.Hour, "How long data persists before expiring")
		statsdAddr    = flag.String("statsdaddr", "localhost:8125", "Address to send UDP StatsD metrics")
		removeCorrupt = flag.Bool("removecorrupt", false, "Whether to skip+delete corrupt chunks on load")
		versionFlag   = flag.Bool("version", false, "Display the version and exit")
	)
	flag.Parse()

	if *versionFlag {
		fmt.Println(version)
		return
	}

	chunkSizeBytes, err := humanize.ParseBytes(*chunkSize)
	if err != nil {
		log.Fatalf("Bad -chunksize %q: %s", *chunkSize, err)
	}

	log.Printf("Now listening on %s (dir=%s; chunksize=%d; expiry=%s)", *addr, *dir, chunkSizeBytes, *expiry)
	server, err := NewServer(*dir, *addr, chunkSizeBytes, *expiry, *statsdAddr, *removeCorrupt)
	if err != nil {
		log.Fatalf("Fatal error opening DB at %s: %v", *dir, err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Printf("Caught signal (%v); shutting down...", <-c)
		if err := server.Stop(); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
