package sync

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/CodisLabs/redis-port/pkg/libs/log"
	"github.com/cupcake/rdb"
	"github.com/cupcake/rdb/nopdecoder"
)

type decoder struct {
	nopdecoder.NopDecoder

	read     int
	position int
	buffer   []byte

	from       string
	target     string
	fromConn   net.Conn
	targetConn net.Conn
	array      [][]byte

	f *os.File
}

const (
	BUFFER_SIZE = 32768
	LEN         = "$%d\r\n"
)

var CRLF = []byte("\r\n")
var SET = []byte("*3\r\n$3\r\nSET\r\n")
var HSET = []byte("*4\r\n$4\r\nHSET\r\n")
var SADD = []byte("*3\r\n$4\r\nSADD\r\n")
var ZADD = []byte("*4\r\n$4\r\nZADD\r\n")
var SYNC = []byte("*1\r\n$4\r\nSYNC\r\n")
var keyLen, valueLen, fieldLen, memberLen, scoreByte, scoreLen []byte

func NewDecoder(from, target string) *decoder {
	d := &decoder{
		from:   from,
		target: target,
		buffer: make([]byte, BUFFER_SIZE),
	}
	fromConn, err := net.Dial("tcp", from)
	if err != nil {
		log.Panic(err, "Init from conn failed")
	}
	d.fromConn = fromConn
	targetConn, err := net.Dial("tcp", target)
	if err != nil {
		log.Panic(err, "Init target conn failed")
	}
	d.targetConn = targetConn
	return d
}

func (self *decoder) Run() {
	filename := fmt.Sprintf("./%s.rdb", self.from)
	f, err := os.Create(filename)
	if err != nil {
		log.Panic(err)
	}
	self.f = f
	self.dump()
	self.f.Seek(0, 0)
	if err := rdb.Decode(self.f, self); err != nil {
		log.Panic(err, " decode rdb stream failed")
	}
	f.Close()
	os.Remove(filename)
	self.aof()
}

func (self *decoder) dump() {
	self.read = 0
	if _, err := self.fromConn.Write(SYNC); err != nil {
		log.Panic(err, "send sync command failed")
	}

	length, err := self.readDumpInfo()
	if err != nil {
		log.Panic(err)
	}

	log.Info("Begin sync rdb from source redis")
	for {
		end := self.position + length
		if end > self.read {
			end = self.read
		}
		if _, err := self.f.Write(self.buffer[self.position:end]); err != nil {
			log.Panic(err, "write to Pipe file failed")
		}
		length -= end - self.position
		if length == 0 {
			self.position = end
			break
		}
		self.read, err = self.fromConn.Read(self.buffer)
		if err != nil {
			log.Panic(err, "read source failed")
		}
		self.position = 0
	}
	log.Info("Sync rdb from source redis finished")
}

func (self *decoder) readDumpInfo() (int, error) {
	for {
		n, err := self.fromConn.Read(self.buffer[self.read:])
		if err != nil {
			return 0, err
		}
		if n == 0 {
			continue
		}
		if n == 1 && self.buffer[0] == '\n' {
			continue
		} //common short circuit

		self.read += n
		for i := 0; i < n; i++ {
			if self.buffer[i] == '$' {
				i++
				for j := i; j < n; j++ {
					if self.buffer[j+1] == '\r' && self.buffer[j+2] == '\n' {
						length, err := strconv.Atoi(string(self.buffer[i : j+1]))
						if err != nil {
							return 0, err
						}
						self.position = j + 3
						return length, nil
					}
				}
			}
		}
		if self.read > 128 {
			return 0, errors.New("Expecting dump length to be in the first 128 bytes")
		}
	}
}

func (self *decoder) aof() {
	log.Info("Start aof stream")
	for {
		if self.position < self.read {
			if _, err := self.targetConn.Write(self.buffer[self.position:self.read]); err != nil {
				log.ErrorError(err, " aof stream failed")
				self.keepAlive(self.buffer[self.position:self.read])
				log.Info(" aof stream restore")
			}
		}
		n, err := self.fromConn.Read(self.buffer)
		if err != nil {
			log.Panic(err, " aof stream failed")
		}
		self.position = 0
		self.read = n
	}
}

func (self *decoder) close() {
	if self.targetConn != nil {
		if err := self.targetConn.Close(); err != nil {
			log.Error(err)
		}
	}
	if self.fromConn != nil {
		if err := self.fromConn.Close(); err != nil {
			log.Error(err)
		}
	}
}

func (self *decoder) StartRDB() {
	log.Info("Start transfer rdb")
}

func (self *decoder) EndRDB() {
	log.Info("Transfer rdb finished")
}

func (self *decoder) keepAlive(arg []byte) {
	self.targetConn.Close()
	for {
		targetConn, err := net.Dial("tcp", self.target)
		if err != nil {
			log.ErrorError(err, "Reconnect target conn failed")
			continue
		}
		self.targetConn = targetConn
		if _, err := self.targetConn.Write(arg); err != nil {
			log.Panic(err, " Network have some problem")
		}
		break
	}
}

func (self *decoder) do(args ...[]byte) {
	for _, arg := range args {
		if _, err := self.targetConn.Write(arg); err != nil {
			log.ErrorError(err, string(arg))
			self.keepAlive(arg)
			log.Info("transfer restore")
		}
	}
}

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

func (self *decoder) StartList(key []byte, length, expiry int64) {
	//self.do(DEL, key, CRLF)
	elemLen := 2 + length
	rpush := []byte(fmt.Sprintf("*%d\r\n$5\r\nRPUSH\r\n", elemLen))
	self.array = [][]byte{}
	keyLen := []byte(fmt.Sprintf(LEN, len(key)))
	del := []byte("*2\r\n$3\r\nDEL\r\n")
	self.array = append(self.array, del, keyLen, key, CRLF, rpush, keyLen, key, CRLF)
}

func (self *decoder) Rpush(key, value []byte) {
	valueLen = []byte(fmt.Sprintf(LEN, len(value)))
	self.array = append(self.array, valueLen, value, CRLF)
}

func (self *decoder) EndList(key []byte) {
	self.do(self.array...)
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	keyLen = []byte(fmt.Sprintf(LEN, len(key)))
	memberLen = []byte(fmt.Sprintf(LEN, len(member)))
	scoreByte = []byte(fmt.Sprintf("%v", score))
	scoreLen = []byte(fmt.Sprintf(LEN, len(scoreByte)))
	self.do(ZADD, keyLen, key, CRLF, scoreLen, scoreByte, CRLF, memberLen, member, CRLF)
}
