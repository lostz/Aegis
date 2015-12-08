package mysql

import (
	"errors"
	"fmt"
)

type MysqlError struct {
	Code    uint16
	Message string
	State   string
}

var (
	ErrBadConn                = errors.New("connection was bad")
	ErrMalformPacket          = errors.New("Malform packet error")
	ErrTxDone                 = errors.New("sql: Transaction has already been committed or rolled back")
	ErrTooManyUserConnections = errors.New("too many connections")
)

func NewDefaultError(code uint16, args ...interface{}) *MysqlError {
	e := new(MysqlError)
	e.Code = code

	if s, ok := MySQLState[code]; ok {
		e.State = s
	} else {
		e.State = DEFAULT_MYSQL_STATE
	}

	if format, ok := MySQLErrName[code]; ok {
		e.Message = fmt.Sprintf(format, args...)
	} else {
		e.Message = fmt.Sprint(args...)
	}

	return e
}

func NewMysqlError(code uint16, message string) *MysqlError {
	e := new(MysqlError)
	e.Code = code

	if s, ok := MySQLState[code]; ok {
		e.State = s
	} else {
		e.State = DEFAULT_MYSQL_STATE
	}

	e.Message = message

	return e

}

func (e *MysqlError) Error() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.Code, e.State, e.Message)
}
