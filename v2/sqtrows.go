package sqtdhlite

import (
	"database/sql"

	"github.com/NarsilWorks-Inc/datahelperlite/v2"
)

// SQLiteRows struct
type SQLiteRows struct {
	pageId    string
	pageCount int
	sqr       *sql.Rows
}

// NewSQLiteRows generates a datahelper compatible SQLServerRows
func NewSQLiteRows(sqlr *sql.Rows) SQLiteRows {
	return SQLiteRows{
		sqr: sqlr,
	}
}

// Close rows
func (ss SQLiteRows) Close() {
	if ss.sqr != nil {
		ss.sqr.Close()
	}
}

// Err check
func (ss SQLiteRows) Err() error {
	return ss.sqr.Err()
}

// Next row in the sequence
func (ss SQLiteRows) Next() bool {
	return ss.sqr.Next()
}

// Scan to destination variables
func (ss SQLiteRows) Scan(dest ...interface{}) error {
	destq := prepareDest(dest)
	err := ss.sqr.Scan(destq...)
	if err != nil {
		return err
	}
	err = copyScannedToDest(dest, destq)
	if err != nil {
		return err
	}
	return nil
}

// Values from the rows
func (ss SQLiteRows) Values() ([]interface{}, error) {
	return nil, nil
}

// Columns from the rows
func (ss SQLiteRows) Columns() ([]datahelperlite.Column, error) {
	cts, err := ss.sqr.ColumnTypes()
	if err != nil {
		return nil, err
	}
	ctps := make([]datahelperlite.Column, len(cts))
	for i, ct := range cts {
		ctps[i] = Column{
			name:    ct.Name(),
			dbtname: ct.DatabaseTypeName(),
			scntyp:  ct.ScanType(),
		}
	}
	return ctps, nil
}

// RawValues from the rows
func (ss SQLiteRows) RawValues() [][]byte {
	return nil
}

// PageID as a result of paged query
func (ss SQLiteRows) PageID() string {
	return ss.pageId
}

// PageCount as a result of a paged query
func (ss SQLiteRows) PageCount() int {
	return ss.pageCount
}
