package main

import (
	"flag"

	"github.com/CMGS/redis-port/sync"
)

func main() {
	var from string
	var target string

	flag.StringVar(&from, "from", "localhost:6379", "from redis")
	flag.StringVar(&target, "to", "localhost:6380", "to redis")
	flag.Parse()
	port := sync.NewDecoder(from, target)
	port.Run()
}
