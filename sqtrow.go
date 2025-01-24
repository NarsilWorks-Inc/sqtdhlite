package sqtdhlite

import (
	"database/sql"
)

// SQLServerRow struct
type SQLServerRow struct {
	sqr *sql.Row
}

// NewSQLServerRow generates a datahelper compatible SQLServerRows
func NewSQLServerRow(sqlr *sql.Row) SQLServerRow {
	return SQLServerRow{
		sqr: sqlr,
	}
}

// Scan to destination variables
func (ss SQLServerRow) Scan(dest ...interface{}) error {
	destq := prepareDest(dest)
	if err := ss.sqr.Scan(destq...); err != nil {
		return err
	}
	if err := copyScannedToDest(dest, destq); err != nil {
		return err
	}
	return nil

}
