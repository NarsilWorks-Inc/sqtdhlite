package sqtdhlite

import (
	"database/sql"

	"github.com/NarsilWorks-Inc/datahelperlite/v3"
)

// SQLiteRows struct
type SQLiteRows struct {
	// pageId    string
	// pageCount int
	sqr *sql.Rows
}

// // NewPostgreSQLRows generates a datahelper compatible PostgreSQLRows
// func NewPostgreSQLRows(sqlr *pgx.Rows) PostgreSQLRows {
// 	return PostgreSQLRows{
// 		sqr: *sqlr,
// 	}
// }

// NewSQLiteRows generates a datahelper compatible PostgreSQLRows
func NewSQLiteRows(sqlr *sql.Rows) SQLiteRows {
	return SQLiteRows{
		sqr: sqlr,
	}
}

// Close rows
func (ss SQLiteRows) Close() error {
	ss.sqr.Close()
	return nil
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
func (ss SQLiteRows) Scan(dest ...any) error {
	destq := prepareDest(dest)
	err := ss.sqr.Scan(destq...)
	if err != nil {
		return err
	}

	// return values
	err = copyScannedToDest(dest, destq)
	if err != nil {
		return err
	}

	return nil
}

// Columns from the rows
func (ss SQLiteRows) Columns() ([]datahelperlite.Column, error) {
	//pgci := pgtype.NewConnInfo()
	// fds := ss.sqr.FieldDescriptions()

	// ctps := make([]datahelperlite.Column, len(fds))
	// for i, ct := range fds {
	// 	dt, _ := pgci.DataTypeForOID(ct.DataTypeOID)
	// 	ctps[i] = Column{
	// 		name:    string(ct.Name),
	// 		dbtname: dt.Name,
	// 		scntyp:  reflect.TypeOf(dt.Value),
	// 	}
	// }

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

// Values from the rows
func (ss SQLiteRows) Values() ([]any, error) {
	return nil, nil
}

// RawValues from the rows
func (ss SQLiteRows) RawValues() [][]byte {
	return nil
}
