package server

import "github.com/lostz/Aegis/mysql"

func (self *Session) isInTransaction() bool {
	return self.status&mysql.SERVER_STATUS_IN_TRANS > 0
}

func (self *Session) isAutoCommit() bool {
	return self.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (self *Session) needBeginTx() bool {
	return self.isInTransaction() || !self.isAutoCommit()
}
