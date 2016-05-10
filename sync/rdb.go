package sync

import (
	"fmt"
	"time"

	"github.com/cupcake/rdb/nopdecoder"
	"github.com/garyburd/redigo/redis"
)

const (
	MAXIMUM_CONN = 1000
)

type decoder struct {
	nopdecoder.NopDecoder
	db  int
	rds *redis.Pool
}

var conn redis.Conn
var err error

func NewDecoder(target string) (*decoder, error) {
	d := &decoder{}
	pool := &redis.Pool{
		MaxIdle:     MAXIMUM_CONN,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", target)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	d.rds = pool
	return d, nil
}

func (self *decoder) StartDatabase(n int) {
	self.db = n
}

func (self *decoder) Set(key, value []byte, expiry int64) {
	conn = self.rds.Get()
	_, err = conn.Do("SET", key, value)
	if err != nil {
		fmt.Println("SET db=%d %q %q", self.db, key, value)
	}
	conn.Close()
}

func (self *decoder) Hset(key, field, value []byte) {
	conn = self.rds.Get()
	_, err = conn.Do("HSET", key, field, value)
	if err != nil {
		fmt.Println("HSET db=%d %q %q %q", self.db, key, field, value)
	}
	conn.Close()
}

func (self *decoder) Sadd(key, member []byte) {
	conn = self.rds.Get()
	_, err = conn.Do("SADD", key, member)
	if err != nil {
		fmt.Println("SADD db=%d %q %q", self.db, key, member)
	}
	conn.Close()
}

func (self *decoder) Rpush(key, value []byte) {
	conn = self.rds.Get()
	_, err = conn.Do("RPUSH", key, value)
	if err != nil {
		fmt.Println("RPUSH db=%d %q %q", self.db, key, value)
	}
	conn.Close()
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	conn = self.rds.Get()
	_, err = conn.Do("ZADD", key, score, member)
	if err != nil {
		fmt.Println("ZADD db=%d %q %f %q", self.db, key, score, member)
	}
	conn.Close()
}
