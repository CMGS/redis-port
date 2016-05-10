package sync

import (
	"fmt"
	"net"

	"github.com/cupcake/rdb/nopdecoder"
)

type decoder struct {
	nopdecoder.NopDecoder
	targetConn net.Conn
}

var err error

func NewDecoder(targetConn net.Conn) (*decoder, error) {
	d := &decoder{targetConn: targetConn}
	return d, nil
}

func (self *decoder) Set(key, value []byte, expiry int64) {
	_, e := fmt.Fprintf(self.targetConn, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	if e != nil {
		fmt.Println(e)
		fmt.Printf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	}
}

func (self *decoder) Hset(key, field, value []byte) {
	_, e := fmt.Fprintf(self.targetConn, "*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(field), field, len(value), value)
	if e != nil {
		fmt.Println(e)
		fmt.Printf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(field), field, len(value), value)
	}
}

func (self *decoder) Sadd(key, member []byte) {
	_, e := fmt.Fprintf(self.targetConn, "*3\r\n$4\r\nSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(member), member)
	if e != nil {
		fmt.Println(e)
		fmt.Printf("*3\r\n$4\r\nSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(member), member)
	}
}

func (self *decoder) Rpush(key, value []byte) {
	_, e := fmt.Fprintf(self.targetConn, "*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	if e != nil {
		fmt.Println(e)
		fmt.Printf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	}
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	sl := len(fmt.Sprintf("%v", score))
	_, e := fmt.Fprintf(self.targetConn, "*4\r\n$4\r\nZADD\r\n$%d\r\n%s\r\n$%d\r\n%v\r\n$%d\r\n%s\r\n", len(key), key, sl, score, len(member), member)
	if e != nil {
		fmt.Println(e)
		fmt.Printf("*4\r\n$4\r\nZADD\r\n$%d\r\n%s\r\n$%d\r\n%f\r\n$%d\r\n%s\r\n", len(key), key, sl, score, len(member), member)
	}
}
