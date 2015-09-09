package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/cespare/kvcache/internal/github.com/garyburd/redigo/redis"
)

// Simple benchmark test. Generate random keys/vals and insert into the DB. For some % of these, wait for some
// amount of time and then query for them.

const (
	minChar = ' '
	maxChar = '~'
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randStr(n int) string {
	r := make([]rune, n)
	for i := range r {
		r[i] = rune(rand.Intn(maxChar-minChar+1) + minChar)
	}
	return string(r)
}

var randomValues [][]byte

func init() {
	for i := 0; i < 1000; i++ {
		randomValues = append(randomValues, []byte(randStr(rand.Intn(1000)+500)))
	}
}

func makeRequests(pool *redis.Pool, delay time.Duration, stats chan<- float64) {
	conn := pool.Get()
	defer conn.Close()

	for i := 0; ; i = (i + 1) % len(randomValues) {
		key := randStr(10)
		val := randomValues[i]
		start := time.Now()
		_, err := conn.Do("SET", key, val, "NX")
		stats <- time.Since(start).Seconds() * 1000
		if err != nil {
			log.Fatal(err)
		}
		//result, err := redis.String(conn.Do("GET", key))
		//if err != nil {
		//log.Fatal(err)
		//}
		//if result != val {
		//log.Fatal("result mismatch")
		//}
		time.Sleep(delay)
	}
}

func main() {
	pool := &redis.Pool{
		MaxIdle:     0,
		MaxActive:   0, // No limit
		IdleTimeout: time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:5533") },
	}

	const P = 4
	const targetQPS = 10000
	delay := time.Second / time.Duration(float64(targetQPS)/float64(P))

	stats := make(chan float64)
	for i := 0; i < P; i++ {
		go func() {
			makeRequests(pool, delay, stats)
		}()
	}
	collectStats(stats)
}

func collectStats(c <-chan float64) {
	tick := time.Tick(30 * time.Second)
	stats := NewStats()
	for {
		select {
		case <-tick:
			fmt.Println(stats)
			stats = NewStats()
		case f := <-c:
			stats.Add(f)
		}
	}
}

var thresholdsMS = []float64{1, 10, 50}

type Stats struct {
	start       time.Time
	max         float64
	total       float64
	samples     float64
	aboveThresh []float64
}

func NewStats() *Stats {
	return &Stats{
		start:       time.Now(),
		aboveThresh: make([]float64, len(thresholdsMS)),
	}
}

func (s *Stats) Add(f float64) {
	if f > s.max {
		s.max = f
	}
	s.total += f
	s.samples++
	for i, thresh := range thresholdsMS {
		if f > thresh {
			s.aboveThresh[i]++
		}
	}
}

func (s *Stats) String() string {
	return fmt.Sprintf("%.0f samples; %.1f qps; max = %.2fms; mean = %.2fms; >%.1fms = %.2f%%; >%.1fms = %.2f%%; >%.1fms = %.2f%%",
		s.samples, s.samples/time.Since(s.start).Seconds(), s.max, s.total/s.samples,
		thresholdsMS[0], (s.aboveThresh[0]/s.total)*100,
		thresholdsMS[1], (s.aboveThresh[1]/s.total)*100,
		thresholdsMS[2], (s.aboveThresh[2]/s.total)*100,
	)
}
