package sync

import (
	"fmt"
	"io"
	"net"

	"github.com/cupcake/rdb/nopdecoder"
)

type decoder struct {
	nopdecoder.NopDecoder
	targetConn net.Conn
	r          *io.PipeReader
	w          *io.PipeWriter
}

var err error

func NewDecoder(targetConn net.Conn) (*decoder, error) {
	d := &decoder{targetConn: targetConn}
	r, w := io.Pipe()
	d.r = r
	d.w = w
	return d, nil
}

func (self *decoder) Send() (int64, error) {
	return io.Copy(self.targetConn, self.r)
}

func (self *decoder) Set(key, value []byte, expiry int64) {
	fmt.Fprintf(self.w, "*3\r\n$3\r\nSET\r\n$%d\r\n%q\r\n$%d\r\n%q\r\n", len(key), key, len(value), value)
}

func (self *decoder) Hset(key, field, value []byte) {
	fmt.Fprintf(self.w, "*4\r\n$4\r\nHSET\r\n$%d\r\n%q\r\n$%d\r\n%q\r\n$%d\r\n%q\r\n", len(key), key, len(field), field, len(value), value)
}

func (self *decoder) Sadd(key, member []byte) {
	fmt.Fprintf(self.w, "*3\r\n$4\r\nSADD\r\n$%d\r\n%q\r\n$%d\r\n%q\r\n", len(key), key, len(member), member)
}

func (self *decoder) Rpush(key, value []byte) {
	fmt.Fprintf(self.w, "*3\r\n$5\r\nRPUSH\r\n$%d\r\n%q\r\n$%d\r\n%q\r\n", len(key), key, len(value), value)
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	sl := len(fmt.Sprintf("%f", score))
	fmt.Fprintf(self.w, "*4\r\n$4\r\nZADD\r\n$%d\r\n%q\r\n$%d\r\n%f\r\n$%d\r\n%q\r\n", len(key), key, sl, score, len(member), member)
}

func (self *decoder) EndRDB() {
	self.r.Close()
	self.w.Close()
}
