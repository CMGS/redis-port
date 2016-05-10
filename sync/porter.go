package sync

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/cupcake/rdb"
)

const (
	BUFFER_SIZE  = 32768
	RDB_FILE_DIR = "."
)

type Porter struct {
	read       int
	position   int
	buffer     []byte
	from       string
	target     string
	fromConn   net.Conn
	targetConn net.Conn
	filename   string
}

func NewPorter(from, target string) *Porter {
	return &Porter{
		from:     from,
		target:   target,
		filename: fmt.Sprintf("%s/%s.rdb", RDB_FILE_DIR, from),
		buffer:   make([]byte, BUFFER_SIZE),
	}
}

func (self *Porter) Run() {
	var conn net.Conn
	var err error

	for {
		if conn, err = self.connect(self.from); err != nil {
			self.failure(err)
			continue
		}
		self.fromConn = conn
		if conn, err = self.connect(self.target); err != nil {
			self.failure(err)
			continue
		}
		self.targetConn = conn
		if err := self.dump(); err != nil {
			self.failure(err)
			continue
		}
		if err := self.transfer(); err != nil {
			self.failure(err)
			continue
		}
		if err := self.aof(); err != nil {
			self.failure(err)
			continue
		}
		break
	}
	self.close()
}

func (self *Porter) failure(err error) {
	self.close()
	fmt.Println(err)
	time.Sleep(5 * time.Second)
}

func (self *Porter) connect(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (self *Porter) transfer() error {
	fmt.Println("begin transfer rdb to redis")
	f, err := os.Open(self.filename)
	if err != nil {
		return err
	}
	d, _ := NewDecoder(self.targetConn)
	//fmt.Println("transfer rdb to redis end")
	//os.Remove(self.filename)
	return rdb.Decode(f, d)
}

func (self *Porter) dump() error {
	fmt.Println("begin sync")
	self.read = 0
	self.fromConn.Write([]byte("*1\r\n$4\r\nSYNC\r\n"))
	length, err := self.readDumpInfo()
	if err != nil {
		return err
	}

	f, err := os.Create(self.filename)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Println("begin dump")
	for {
		end := self.position + length
		if end > self.read {
			end = self.read
		}
		if _, err := f.Write(self.buffer[self.position:end]); err != nil {
			return err
		}
		length -= end - self.position
		if length == 0 {
			self.position = end
			break
		}
		self.read, err = self.fromConn.Read(self.buffer)
		if err != nil {
			return err
		}
		self.position = 0
	}
	fmt.Println("dump end")

	return nil
}

func (self *Porter) readDumpInfo() (int, error) {
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

func (self *Porter) aof() error {
	fmt.Println("begin aof stream")
	for {
		select {
		default:
			if self.position < self.read {
				self.targetConn.Write(self.buffer[self.position:self.read])
			}
			n, err := self.fromConn.Read(self.buffer)
			if err != nil {
				return err
			}
			self.position = 0
			self.read = n
		}
	}
}

func (self *Porter) close() {
	if self.fromConn != nil {
		self.fromConn.Close()
	}
	if self.targetConn != nil {
		self.targetConn.Close()
	}
}
