package sqtdhlite

import (
	"database/sql"

	"github.com/NarsilWorks-Inc/datahelperlite"
)

// SQLServerRows struct
type SQLServerRows struct {
	pageId    string
	pageCount int
	sqr       *sql.Rows
}

// NewSQLServerRows generates a datahelper compatible SQLServerRows
func NewSQLServerRows(sqlr *sql.Rows) SQLServerRows {
	return SQLServerRows{
		sqr: sqlr,
	}
}

// Close rows
func (ss SQLServerRows) Close() {
	if ss.sqr != nil {
		ss.sqr.Close()
	}
}

// Err check
func (ss SQLServerRows) Err() error {
	return ss.sqr.Err()
}

// Next row in the sequence
func (ss SQLServerRows) Next() bool {
	return ss.sqr.Next()
}

// Scan to destination variables
func (ss SQLServerRows) Scan(dest ...interface{}) error {
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
func (ss SQLServerRows) Values() ([]interface{}, error) {
	return nil, nil
}

// Columns from the rows
func (ss SQLServerRows) Columns() ([]datahelperlite.Column, error) {
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
func (ss SQLServerRows) RawValues() [][]byte {
	return nil
}

// PageID as a result of paged query
func (ss SQLServerRows) PageID() string {
	return ss.pageId
}

// PageCount as a result of a paged query
func (ss SQLServerRows) PageCount() int {
	return ss.pageCount
}
