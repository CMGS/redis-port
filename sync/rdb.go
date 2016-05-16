package sync

import (
	"bytes"
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
	listBuff   *bytes.Buffer

	f *os.File
}

const (
	BUFFER_SIZE = 32768
	LEN         = "$%d\r\n"
	SET         = "*3\r\n$3\r\nSET\r\n"
	HSET        = "*4\r\n$4\r\nHSET\r\n"
	SADD        = "*3\r\n$4\r\nSADD\r\n"
	ZADD        = "*4\r\n$4\r\nZADD\r\n"
	DEL         = "*2\r\n$3\r\nDEL\r\n"
	CRLF        = "\r\n"
)

var SYNC = []byte("*1\r\n$4\r\nSYNC\r\n")

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
			log.Panic(err, " network have some problem")
		}
		break
	}
}

func (self *decoder) do(b []byte) {
	if _, err := self.targetConn.Write(b); err != nil {
		log.ErrorError(err, string(b))
		self.keepAlive(b)
		log.Info("transfer restore")
	}
}

func (self *decoder) Set(key, value []byte, expiry int64) {
	buffer := bytes.NewBufferString(SET)
	buffer.WriteString(fmt.Sprintf(LEN, len(key)))
	buffer.Write(key)
	buffer.WriteString(CRLF)
	buffer.WriteString(fmt.Sprintf(LEN, len(value)))
	buffer.Write(value)
	buffer.WriteString(CRLF)
	self.do(buffer.Bytes())
}

func (self *decoder) Hset(key, field, value []byte) {
	buffer := bytes.NewBufferString(HSET)
	buffer.WriteString(fmt.Sprintf(LEN, len(key)))
	buffer.Write(key)
	buffer.WriteString(CRLF)
	buffer.WriteString(fmt.Sprintf(LEN, len(field)))
	buffer.Write(field)
	buffer.WriteString(CRLF)
	buffer.WriteString(fmt.Sprintf(LEN, len(value)))
	buffer.Write(value)
	buffer.WriteString(CRLF)
	self.do(buffer.Bytes())
}

func (self *decoder) Sadd(key, member []byte) {
	buffer := bytes.NewBufferString(SADD)
	buffer.WriteString(fmt.Sprintf(LEN, len(key)))
	buffer.Write(key)
	buffer.WriteString(CRLF)
	buffer.WriteString(fmt.Sprintf(LEN, len(member)))
	buffer.Write(member)
	buffer.WriteString(CRLF)
	self.do(buffer.Bytes())
}

func (self *decoder) StartList(key []byte, length, expiry int64) {
	keyLen := fmt.Sprintf(LEN, len(key))
	self.listBuff = bytes.NewBufferString(DEL)
	self.listBuff.WriteString(keyLen)
	self.listBuff.Write(key)
	self.listBuff.WriteString(CRLF)
	elemLen := 2 + length
	rpush := fmt.Sprintf("*%d\r\n$5\r\nRPUSH\r\n", elemLen)
	self.listBuff.WriteString(rpush)
	self.listBuff.WriteString(keyLen)
	self.listBuff.Write(key)
	self.listBuff.WriteString(CRLF)
}

func (self *decoder) Rpush(key, value []byte) {
	self.listBuff.WriteString(fmt.Sprintf(LEN, len(value)))
	self.listBuff.Write(value)
	self.listBuff.WriteString(CRLF)
}

func (self *decoder) EndList(key []byte) {
	self.do(self.listBuff.Bytes())
}

func (self *decoder) Zadd(key []byte, score float64, member []byte) {
	buffer := bytes.NewBufferString(ZADD)
	scoreStr := fmt.Sprintf("%v", score)
	scoreLen := fmt.Sprintf(LEN, len(scoreStr))
	buffer.WriteString(fmt.Sprintf(LEN, len(key)))
	buffer.Write(key)
	buffer.WriteString(CRLF)
	buffer.WriteString(scoreLen)
	buffer.WriteString(scoreStr)
	buffer.WriteString(CRLF)
	buffer.WriteString(fmt.Sprintf(LEN, len(member)))
	buffer.Write(member)
	buffer.WriteString(CRLF)
	self.do(buffer.Bytes())
}
