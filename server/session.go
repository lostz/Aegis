package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/lostz/Aegis/backend"
	"github.com/lostz/Aegis/mysql"
	"github.com/lostz/Aegis/utils"
)

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION
var baseConnId uint32 = 10000

type Session struct {
	sync.Mutex
	pkg          *mysql.Packets
	c            net.Conn
	capability   uint32
	connectionId uint32
	status       uint16
	collation    mysql.CollationId
	charset      string
	user         string
	db           string
	salt         []byte
	affectedRows int64
	s            *Server
	closed       bool
}

func (self *Session) Run() {
	for {
		data, err := self.pkg.ReadPacket()
		if err != nil {
			return
		}
		if err := self.dispatch(data); err != nil {
			logger.Errorf("dispath %s %d", err.Error(), self.connectionId)
			self.writeError(err)
		}

		if self.closed {
			return
		}

		self.pkg.Sequence = 0

	}

}

func (self *Session) Close() error {
	self.c.Close()
	return nil

}

func (self *Session) dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	switch cmd {
	case mysql.COM_QUIT:
		self.Close()
		return nil
	case mysql.COM_QUERY:
		return self.handleQuery(utils.String(data))
	case mysql.COM_INIT_DB:
		if err := self.useDB(utils.String(data)); err != nil {
			return err
		} else {
			return self.writeOK(nil)
		}
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		logger.Errorf("ClientConn", "dispatch", msg, 0)
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR, msg)

	}
	return nil
}

func (self *Session) Handshake() error {
	if err := self.writeInitialHandshake(); err != nil {
		logger.Errorf("server", "Handshake", err.Error(),
			self.connectionId, "msg", "send initial handshake error")
		return err
	}

	if err := self.readHandshakeResponse(); err != nil {
		logger.Errorf("server", "readHandshakeResponse",
			err.Error(), self.connectionId,
			"msg", "read Handshake Response error")

		self.writeError(err)

		return err
	}

	if err := self.writeOK(nil); err != nil {
		logger.Errorf("server", "readHandshakeResponse",
			"write ok fail",
			self.connectionId, "error", err.Error())
		return err
	}

	self.pkg.Sequence = 0
	return nil
}

func (self *Session) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(self.connectionId), byte(self.connectionId>>8), byte(self.connectionId>>16), byte(self.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, self.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(self.status), byte(self.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, self.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return self.pkg.WritePacket(data)
}

func (self *Session) readHandshakeResponse() error {
	data, err := self.pkg.ReadPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	self.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	self.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(self.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	checkAuth := mysql.CalcPassword(self.salt, []byte(self.s.cfg.Password))
	if !bytes.Equal(auth, checkAuth) {
		logger.Errorf("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"passworld", self.s.cfg.Password)
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, self.c.RemoteAddr().String(), self.user, "Yes")
	}

	pos += authLen

	if self.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(self.db) + 1

		//if db name is "", use default db
		if db == "" {
			db = self.s.cfg.DB
		}

		if err := self.useDB(db); err != nil {
			return err
		}
	}

	return nil
}

func (self *Session) useDB(db string) error {
	self.db = db
	return nil
}

func (self *Session) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: self.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return self.pkg.WritePacket(data)
}

func (self *Session) writeError(e error) error {
	var m *mysql.MysqlError
	var ok bool
	if m, ok = e.(*mysql.MysqlError); !ok {
		m = mysql.NewMysqlError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return self.pkg.WritePacket(data)
}

func (self *Session) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if self.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return self.pkg.WritePacket(data)
}

func (self *Session) checkDB() error {
	return self.useDB(self.db)
}

func (self *Session) getReader() (backend.Client, error) {
	conn, err := self.s.readerPool.Get()
	if err != nil {
		logger.Errorf("get reader conn", err.Error())
		return nil, err
	}
	return conn.(backend.Client), nil

}

func (self *Session) getWriter() (backend.Client, error) {
	conn, err := self.s.writerPool.Get()
	if err != nil {
		logger.Errorf("get writer conn", err.Error())
		return nil, err
	}
	return conn.(backend.Client), nil
}

func (self *Session) getConn(isSelect bool) (backend.Client, error) {
	if isSelect {
		return self.getReader()
	} else {
		return self.getWriter()
	}

}
