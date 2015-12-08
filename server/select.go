package server

import (
	"fmt"

	"github.com/lostz/Aegis/backend"
	"github.com/lostz/Aegis/mysql"
	"github.com/lostz/Aegis/sql"
)

func (self *Session) handleSelect(stmt sql.IStatement, sqlstmt string) error {
	if err := self.checkDB(); err != nil {
		return err
	}

	isread := false
	if s, ok := stmt.(sql.ISelect); ok {
		isread = !s.IsLocked()
	} else if _, sok := stmt.(sql.IShow); sok {
		isread = true
	}
	conn, err := self.getConn(isread)

	if err != nil {
		return err
	} else if conn == nil {
		return fmt.Errorf("no available connection")
	}

	var res *mysql.Result
	res, err = conn.Execute(sqlstmt)

	self.closeDBConn(conn, false)
	if err == nil {
		err = self.mergeSelectResult(res)
	}
	return err
}

func (self *Session) closeDBConn(c backend.Client, rollback bool) {
	if self.isInTransaction() || !self.isAutoCommit() {
		return
	}
	if rollback {
		c.Rollback()
	}
	c.Close()
}

func (self *Session) mergeSelectResult(rs *mysql.Result) error {
	r := rs.Resultset
	status := self.status | rs.Status
	return self.writeResultset(status, r)
}
