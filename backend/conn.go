package backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	"github.com/lostz/Aegis/mysql"
)

type Client interface {
	Connect(addr string, user string, password string, db string) error
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Rollback() error
	Close() error
}

type Conn struct {
	conn net.Conn

	pkg *mysql.Packets

	addr     string
	user     string
	password string
	db       string

	capability uint32

	status uint16

	collation mysql.CollationId
	charset   string
	salt      []byte

	lastPing int64

	pkgErr error
}

func (self *Conn) Connect(addr string, user string, password string, db string) error {
	self.addr = addr
	self.user = user
	self.password = password
	self.db = db

	//use utf8
	self.collation = mysql.DEFAULT_COLLATION_ID
	self.charset = mysql.DEFAULT_CHARSET

	return self.ReConnect()
}

func (self *Conn) ReConnect() error {
	if self.conn != nil {
		self.conn.Close()
	}

	conn, err := net.Dial("tcp", self.addr)
	if err != nil {
		return err
	}

	self.conn = conn
	self.pkg = mysql.NewPackets(conn)

	if err := self.readInitialHandshake(); err != nil {
		self.conn.Close()
		return err
	}

	if err := self.writeAuthHandshake(); err != nil {
		self.conn.Close()

		return err
	}

	if _, err := self.readOK(); err != nil {
		self.conn.Close()

		return err
	}

	//we must always use autocommit
	if !self.IsAutoCommit() {
		if _, err := self.exec("set autocommit = 1"); err != nil {
			self.conn.Close()

			return err
		}
	}

	return nil
}

func (self *Conn) readInitialHandshake() error {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return err
	}

	if data[0] == mysql.ERR_HEADER {
		return errors.New("read initial handshake error")
	}

	if data[0] < mysql.MinProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must >= 10", data[0])
	}

	//skip mysql version and connection id
	//mysql version end with 0x00
	//connection id length is 4
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	self.salt = append(self.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	self.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		self.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		self.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | self.capability

		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		self.salt = append(self.salt, data[pos:pos+12]...)
	}

	return nil
}

func (self *Conn) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SECURE_CONNECTION |
		mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_LONG_FLAG

	capability &= self.capability

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(self.user) + 1

	//we only support secure connection
	auth := mysql.CalcPassword(self.salt, []byte(self.password))

	length += 1 + len(auth)

	if len(self.db) > 0 {
		capability |= mysql.CLIENT_CONNECT_WITH_DB

		length += len(self.db) + 1
	}

	self.capability = capability

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	//MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	//Charset [1 byte]
	data[12] = byte(self.collation)

	//Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	//User [null terminated string]
	if len(self.user) > 0 {
		pos += copy(data[pos:], self.user)
	}
	//data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	// db [null terminated string]
	if len(self.db) > 0 {
		pos += copy(data[pos:], self.db)
		//data[pos] = 0x00
	}

	return self.pkg.WritePacket(data)
}

func (self *Conn) Execute(command string, args ...interface{}) (*mysql.Result, error) {
	if len(args) == 0 {
		return self.exec(command)
	} else {
		if s, err := self.Prepare(command); err != nil {
			return nil, err
		} else {
			var r *mysql.Result
			r, err = s.Execute(args...)
			s.Close()
			return r, err
		}
	}
}

func (self *Conn) Close() error {

	return nil

}

func (self *Conn) Rollback() error {
	_, err := self.exec("rollback")
	return err
}

func (self *Conn) exec(query string) (*mysql.Result, error) {
	if err := self.writeCommandStr(mysql.COM_QUERY, query); err != nil {
		return nil, err
	}

	return self.readResult(false)
}

func (self *Conn) writeCommandStr(command byte, arg string) error {
	self.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return self.pkg.WritePacket(data)
}

func (self *Conn) writeCommandUint32(command byte, arg uint32) error {
	self.pkg.Sequence = 0

	return self.pkg.WritePacket([]byte{
		0x05, //5 bytes long
		0x00,
		0x00,
		0x00, //sequence

		command,

		byte(arg),
		byte(arg >> 8),
		byte(arg >> 16),
		byte(arg >> 24),
	})
}

func (self *Conn) readOK() (*mysql.Result, error) {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == mysql.OK_HEADER {
		return self.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, self.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (self *Conn) readResult(binary bool) (*mysql.Result, error) {
	data, err := self.pkg.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == mysql.OK_HEADER {
		return self.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, self.handleErrorPacket(data)
	} else if data[0] == mysql.LocalInFile_HEADER {
		return nil, mysql.ErrMalformPacket
	}

	return self.readResultset(data, binary)
}

func (self *Conn) isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOF_HEADER && len(data) <= 5
}

func (self *Conn) handleOKPacket(data []byte) (*mysql.Result, error) {
	var n int
	var pos int = 1

	r := new(mysql.Result)

	r.AffectedRows, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		self.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		//Warnings := binary.LittleEndian.Uint16(data[pos:])
		//pos += 2
	} else if self.capability&mysql.CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		self.status = r.Status
		pos += 2
	}

	//info
	return r, nil
}

func (self *Conn) handleErrorPacket(data []byte) error {
	e := new(mysql.MysqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (self *Conn) readResultset(data []byte, binary bool) (*mysql.Result, error) {
	result := &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,

		Resultset: &mysql.Resultset{},
	}

	// column count
	count, _, n := mysql.LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result.Fields = make([]*mysql.Field, count)
	result.FieldNames = make(map[string]int, count)

	if err := self.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := self.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (self *Conn) readResultColumns(result *mysql.Result) (err error) {
	var i int = 0
	var data []byte

	for {
		data, err = self.pkg.ReadPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if self.isEOFPacket(data) {
			if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				self.status = result.Status
			}

			if i != len(result.Fields) {
				err = mysql.ErrMalformPacket
			}

			return
		}

		result.Fields[i], err = mysql.FieldData(data).Parse()
		if err != nil {
			return
		}

		result.FieldNames[string(result.Fields[i].Name)] = i

		i++
	}
}

func (self *Conn) readResultRows(result *mysql.Result, isBinary bool) (err error) {
	var data []byte

	for {
		data, err = self.pkg.ReadPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if self.isEOFPacket(data) {
			if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				self.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Conn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = self.pkg.ReadPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if self.isEOFPacket(data) {
			return
		}
	}
	return
}

func (self *Conn) IsAutoCommit() bool {
	return self.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}
