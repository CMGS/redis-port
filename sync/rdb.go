package sync

import (
	"fmt"
	"net"

	"github.com/cupcake/rdb/nopdecoder"
)

type decoder struct {
	nopdecoder.NopDecoder
	target     string
	targetConn net.Conn
}

var err error
var CRLF = []byte("\r\n")
var SET = []byte("*3\r\n$3\r\nSET\r\n")
var HSET = []byte("*4\r\n$4\r\nHSET\r\n")
var SADD = []byte("*3\r\n$4\r\nSADD\r\n")
var RPUSH = []byte("*3\r\n$5\r\nRPUSH\r\n")
var ZADD = []byte("*4\r\n$4\r\nZADD\r\n")

const LEN = "$%d\r\n"

func NewDecoder(target string) *decoder {
	d := &decoder{target: target}
	d.connect()
	return d
}

func (self *decoder) close() {
	if err := self.targetConn.Close(); err != nil {
		fmt.Println(err)
	}
}

func (self *decoder) connect() {
	conn, err := net.Dial("tcp", self.target)
	if err != nil {
		fmt.Println("connect failed", err)
		return
	}
	self.targetConn = conn
}

func (self *decoder) StartRDB() {
	fmt.Println("Start transfer rdb")
}

func (self *decoder) EndRDB() {
	fmt.Println("Transfer rdb finished")
	self.close()
}

func (self *decoder) do(args ...[]byte) {
	for _, arg := range args {
		if _, err := self.targetConn.Write(arg); err != nil {
			fmt.Print(err, string(arg))
			self.close()
			self.connect()
			fmt.Println(self.targetConn.Write(arg))
			return
		}
	}
}

var keyLen, valueLen, fieldLen, memberLen, scoreByte, scoreLen []byte

func (self *decoder) Set(key, value []byte, expiry int64) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	valueLen = []byte(fmt.Sprintf(LEN, len(value)))
	self.do(SET, keyLen, key, CRLF, valueLen, value, CRLF)
}

func (self *decoder) Hset(key, field, value []byte) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	fieldLen = []byte(fmt.Sprintf(LEN, len(field)))
	valueLen = []byte(fmt.Sprintf(LEN, len(value)))
	self.do(HSET, keyLen, key, CRLF, fieldLen, field, CRLF, valueLen, value, CRLF)
}

func (self *decoder) Sadd(key, member []byte) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	memberLen = []byte(fmt.Sprintf(LEN, len(member)))
	self.do(SADD, keyLen, key, CRLF, memberLen, member, CRLF)
}

func (self *decoder) Rpush(key, value []byte) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	valueLen = []byte(fmt.Sprintf(LEN, len(value)))
	self.do(RPUSH, keyLen, key, CRLF, valueLen, value, CRLF)
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	memberLen = []byte(fmt.Sprintf(LEN, len(member)))
	scoreByte = []byte(fmt.Sprintf("%v", score))
	scoreLen = []byte(fmt.Sprintf(LEN, len(scoreByte)))
	self.do(ZADD, keyLen, key, CRLF, scoreLen, scoreByte, CRLF, memberLen, member, CRLF)
}
