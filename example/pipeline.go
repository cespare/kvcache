package main

import (
	"fmt"
	"log"

	"github.com/cespare/kvcache/internal/github.com/garyburd/redigo/redis"
)

func main() {
	const addr = "localhost:5533"
	c, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	send(c,
		[]string{"PING"},
		[]string{"SET", "foo", "bar"},
		[]string{"PING"},
		[]string{"GET", "foo"},
	)
}

func send(c redis.Conn, commands ...[]string) {
	for _, cmd := range commands {
		args := make([]interface{}, len(cmd)-1)
		for i, arg := range cmd[1:] {
			args[i] = arg
		}
		if err := c.Send(cmd[0], args...); err != nil {
			log.Fatal(err)
		}
	}
	if err := c.Flush(); err != nil {
		log.Fatal(err)
	}
	for _ = range commands {
		v, err := c.Receive()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\033[01;34m>>>> v: %q\x1B[m\n", v)
	}
}
