package server

import (
	"fmt"

	"github.com/lostz/Aegis/sql"
	"github.com/lostz/Aegis/utils"
)

func (self *Session) handleQuery(sqlstmt string) (err error) {
	var stmt sql.IStatement
	stmt, err = sql.Parse(sqlstmt)
	if err != nil {
		return fmt.Errorf(`parse sql "%s" error "%s"`, sqlstmt, err.Error())
	}

	switch v := stmt.(type) {
	case sql.ISelect:
		return self.handleSelect(v, sqlstmt)
		//	case *sql.Insert:
		//		return self.handleExec(stmt, sqlstmt, false)
		//	case *sql.Update:
		//		return self.handleExec(stmt, sqlstmt, false)
		//	case *sql.Delete:
		//		return self.handleExec(stmt, sqlstmt, false)
		//	case *sql.Replace:
		//		return self.handleExec(stmt, sqlstmt, false)
		//	case *sql.Set:
		//		return self.handleSet(v, sqlstmt)
		//	case *sql.Begin:
		//		return self.handleBegin()
		//	case *sql.Commit:
		//		return self.handleCommit()
		//	case *sql.Rollback:
		//		return self.handleRollback()
	case sql.IShow:
		return self.handleShow(sqlstmt, v)
	//	case sql.IDDLStatement:
	//		return self.handleExec(stmt, sqlstmt, false)
	//	case *sql.Do:
	//		return self.handleExec(stmt, sqlstmt, false)
	//	case *sql.Call:
	//		return selfc.handleExec(stmt, sqlstmt, false)
	case *sql.Use:
		if err := self.useDB(utils.String(stmt.(*sql.Use).DB)); err != nil {
			return err
		} else {
			return self.writeOK(nil)
		}

	default:
		return fmt.Errorf("statement %T[%s] not support now", stmt, sqlstmt)
	}

	return nil
}
