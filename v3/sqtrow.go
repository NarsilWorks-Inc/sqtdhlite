package sqtdhlite

import (
	"database/sql"
)

// SQLiteRow struct
type SQLiteRow struct {
	sqr *sql.Row
}

// NewSQLiteRow generates a datahelper compatible SQLServerRows
func NewSQLiteRow(sqlr *sql.Row) SQLiteRow {
	return SQLiteRow{
		sqr: sqlr,
	}
}

// Scan to destination variables
func (ss SQLiteRow) Scan(dest ...interface{}) error {
	destq := prepareDest(dest)
	if err := ss.sqr.Scan(destq...); err != nil {
		return err
	}
	if err := copyScannedToDest(dest, destq); err != nil {
		return err
	}
	return nil

}
