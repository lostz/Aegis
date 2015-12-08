package server

import "github.com/lostz/Aegis/sql"

func (self *Session) handleShow(strsql string, stmt sql.IShow) error {
	var err error

	switch stmt.(type) {
	case *sql.ShowAegisStatus:
		err = self.handleProxyStatus()
	default:
		err = self.handleSelect(stmt, strsql)
	}

	return err

}

func (self *Session) handleProxyStatus() error {
	var Column = 4
	var rows [][]string

	var names []string = []string{
		"Version",
		"CommitID",
		"Master",
		"Slaves",
	}
	rows = append(
		rows,
		[]string{
			self.s.Version,
			self.s.CommitId,
			self.s.cfg.DBAddr,
			self.s.cfg.DBAddr,
		},
	)
	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}
	result, err := self.buildResultset(names, values)
	if err != nil {
		return err
	}
	if result != nil {
		return self.writeResultset(self.status, result)
	}

	return self.writeOK(nil)

}
