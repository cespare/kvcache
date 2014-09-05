package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// TODO: Logging should be configurable (Server has a modifiable log.Logger inside).

type Server struct {
	addr string
	db   *DB
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
		db:   NewDB(),
	}
}

type Request struct {
	Type  RequestType
	Key   []byte
	Value []byte
	Resp  chan *Response
	Err   error // If there was an error reading the request, only this field is set.
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

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.loop(l)
}

func (s *Server) loop(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				delay := 10 * time.Millisecond
				log.Printf("Accept error: %s; retrying in %s", e, delay)
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
				resp.Msg = []byte(r.Err.Error())
				resp.Type = RedisErr
			} else {
				switch r.Type {
				case RequestSet:
					if err := s.db.Put(r.Key, r.Value); err != nil {
						resp = ResponseFromError(err)
						break
					}
					resp.Msg = []byte("OK")
				case RequestGet:
					v, err := s.db.Get(r.Key)
					switch err {
					case nil:
						resp.Type = RedisBulk
						resp.Msg = v
					case ErrKeyNotExist:
						// Redis null value
						resp.Type = RedisBulk
					default:
						resp = ResponseFromError(err)
					}
				case RequestPing:
					resp.Msg = []byte("PONG")
				case RequestInfo:
					resp.Msg = []byte("useful info")
				default:
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
		requests <- &r
		select {
		case <-writeErr:
			return
		default:
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
)

func (r *Request) Parse(br *bufio.Reader) error {
	array, err := parseRedisArrayBulkString(br)
	if err != nil {
		return err
	}
	if len(array) == 0 {
		return ErrMalformedRequest
	}
	switch strings.ToLower(array[0]) {
	case "set":
		if len(array) != 3 {
			return ErrWrongNumArgs
		}
		r.Type = RequestSet
		r.Key = []byte(array[1])
		r.Value = []byte(array[2])
	case "get":
		if len(array) != 2 {
			return ErrWrongNumArgs
		}
		r.Type = RequestGet
		r.Key = []byte(array[1])
	case "ping":
		if len(array) != 1 {
			return ErrWrongNumArgs
		}
		r.Type = RequestPing
	case "info":
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
	const addr = "localhost:5533"
	log.Println("Now listening on", addr)
	log.Fatal(NewServer(addr).Start())
}