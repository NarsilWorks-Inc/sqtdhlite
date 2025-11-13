package sqtdhlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v2"
	dn "github.com/eaglebush/datainfo"
	_ "modernc.org/sqlite"
)

// SQLiteHelper implements DataHelperLite for SQLite
type SQLiteHelper struct {
	db   *sql.DB
	tx   *sql.Tx
	conn *sql.Conn
	dbi  *dn.DataInfo
	ctx  context.Context
	trCnt,
	reuseCnt uint8
	rw  sync.RWMutex
	err error
	rollbackTriggered,
	committed bool
	trnIdMap  map[int8]bool
	lastTrnId int8
}

func init() {
	dhl.SetHelper(`sqtdhlite`, &SQLiteHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *SQLiteHelper) NewHelper() dhl.DataHelperLite {
	return &SQLiteHelper{}
}

// Open a new connection
func (h *SQLiteHelper) Open(ctx context.Context, di *dn.DataInfo) error {

	// If Sql handle and connection is valid
	if h.db != nil && h.conn != nil {
		h.rw.Lock()
		h.reuseCnt++
		h.rw.Unlock()
		return nil
	}

	h.err = nil
	h.dbi = di
	if ctx == nil {
		ctx = context.Background()
	}
	h.ctx = ctx

	if h.db == nil {
		h.db, h.err = sql.Open(`sqlite`, *di.ConnectionString)
		if h.err != nil {
			h.err = fmt.Errorf("open: %w", h.err)
			return h.err
		}
		if di.MaxOpenConnection != nil {
			h.db.SetMaxOpenConns(*di.MaxOpenConnection)
		}
		if di.MaxIdleConnection != nil {
			h.db.SetMaxIdleConns(*di.MaxIdleConnection)
		}
		if di.MaxConnectionLifetime != nil {
			h.db.SetConnMaxLifetime(time.Duration(*di.MaxConnectionLifetime))
		}
		if di.MaxConnectionIdleTime != nil {
			h.db.SetConnMaxIdleTime(time.Duration(*di.MaxConnectionIdleTime))
		}
	}

	if h.conn == nil {
		h.conn, h.err = h.db.Conn(h.ctx)
		if h.err != nil {
			h.err = fmt.Errorf("open: %w", h.err)
			return h.err
		}
	}

	h.rw.Lock()
	h.reuseCnt = 0
	h.rw.Unlock()
	return nil
}

// Close the helper
func (h *SQLiteHelper) Close() error {

	if h.conn == nil {
		return nil
	}

	// if reused, closing will be prevented
	// until reusing is zero
	if h.reuseCnt > 0 {
		h.rw.Lock()
		h.reuseCnt--
		h.rw.Unlock()
		return nil
	}

	// check if transaction exists,
	// rollback if it exists
	if h.tx != nil {
		h.Rollback()
	}

	if h.err = h.conn.Close(); h.err != nil {
		return h.err
	}

	h.rw.Lock()
	defer h.rw.Unlock()
	h.trCnt = 0
	h.conn = nil
	h.err = nil
	h.trnIdMap = nil
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (h *SQLiteHelper) Begin() error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		h.err = fmt.Errorf("begin: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, &sql.TxOptions{})
		if h.err != nil {
			h.err = fmt.Errorf("begin: %w", h.err)
			return h.err
		}
	}
	// Increment transaction count
	// The transaction count will serve as the key for the new map value, set to 1
	// Move the new index to the forward position
	h.rw.Lock()
	defer h.rw.Unlock()
	h.trCnt++
	h.committed = false         // ? Reset commit state
	h.rollbackTriggered = false // ? Reset rollback state

	// Set trn id flag up
	if h.trCnt > 1 {
		if h.trnIdMap == nil {
			h.trnIdMap = make(map[int8]bool)
		}
		h.lastTrnId++
		h.trnIdMap[h.lastTrnId] = true
	}

	return nil
}

// BeginManually begins a transaction that does not support deferred rollback.
func (h *SQLiteHelper) BeginManually() error {
	if h.err != nil {
		return h.err
	}
	if h.db == nil || h.conn == nil {
		h.err = fmt.Errorf("begin-manually: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.tx, h.err = h.conn.BeginTx(h.ctx, &sql.TxOptions{})
		if h.err != nil {
			h.err = fmt.Errorf("begin-manually: %w", h.err)
			return h.err
		}
	}
	// Increment transaction count
	h.rw.Lock()
	defer h.rw.Unlock()
	h.trCnt++
	h.committed = false         // Reset commit state
	h.rollbackTriggered = false // Reset rollback state
	h.lastTrnId = 0
	h.trnIdMap = nil
	return nil
}

func (h *SQLiteHelper) Commit() error {

	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.rollbackTriggered || h.committed {
		return nil
	}

	// If there is an error, we give the control to rollback
	if h.err != nil {
		return h.Rollback()
	}

	h.rw.Lock()
	defer h.rw.Unlock()

	// If the transaction is not the outermost transaction, reduce transaction count.
	if h.trCnt > 1 {
		// If this transaction was called with Begin(), this is a deferred rollback
		// Record the last transaction id (via count) and set the map to false
		// Then reduce the number of transaction count
		if h.trnIdMap != nil {
			h.trnIdMap[h.lastTrnId] = false
		}
		h.trCnt--
		return nil
	}
	// Ensure DB, connection, and transaction are valid before committing
	if h.conn == nil {
		h.err = fmt.Errorf("commit: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("commit: %w", dhl.ErrNoTx)
		return h.err
	}

	// Commit the outermost transaction
	if h.err = h.tx.Commit(); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
		h.err = fmt.Errorf("commit: %w", h.err)
		return h.err
	}

	// Reset transaction state after a successful commit
	h.tx = nil
	h.trCnt = 0
	h.lastTrnId = 0
	h.trnIdMap = nil
	h.committed = true
	h.rollbackTriggered = false

	return nil
}

func (h *SQLiteHelper) Rollback() error {

	// Return early if any of the conditions are true
	if h.tx == nil || h.trCnt == 0 || h.committed {
		return nil
	}

	if h.err != nil {
		return h.rollbk()
	}

	// If trnId's flag was off, return early
	// This only applies to deferred rollbacks
	if h.trnIdMap != nil && !h.trnIdMap[h.lastTrnId] {
		h.lastTrnId--
		return nil
	}

	// If the transaction is not the first transaction, reduce the transaction count
	if h.trCnt > 1 {
		h.trCnt--
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return h.rollbk()
}

func (h *SQLiteHelper) rollbk() error {
	if h.committed {
		return nil // ?? If already committed, skip rollback
	}

	// Ensure DB, connection, and transaction are valid before rolling back
	if h.conn == nil {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("rollback: %w", dhl.ErrNoTx)
		return h.err
	}

	h.rw.Lock()
	h.rollbackTriggered = true // 🔧 Mark rollback occurred
	h.rw.Unlock()

	// Perform rollback
	if h.err = h.tx.Rollback(); h.err != nil && !errors.Is(h.err, sql.ErrTxDone) {
		h.err = fmt.Errorf("rollback: %w", h.err)
		return h.err
	}

	// Reset all transaction state after rollback
	h.rw.Lock()
	defer h.rw.Unlock()
	h.tx = nil
	h.trCnt = 0
	h.err = nil
	h.committed = false         // ?? Reset flags
	h.rollbackTriggered = false // ?? Reset flags (rollback is done)

	return nil
}

// Mark a savepoint
func (h *SQLiteHelper) Mark(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("mark: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("mark: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `SAVEPOINT sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("mark: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Discard a savepoint
func (h *SQLiteHelper) Discard(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("discard: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("discard: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `ROLLBACK TO sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("discard: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Save a savepoint
func (h *SQLiteHelper) Save(name string) error {
	if h.err != nil {
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("save: %w", dhl.ErrNoConn)
		return h.err
	}
	if h.tx == nil {
		h.err = fmt.Errorf("save: %w", dhl.ErrNoTx)
		return h.err
	}
	if h.trCnt > 0 {
		_, h.err = h.tx.ExecContext(h.ctx, `RELEASE TO sp_`+name+`;`)
		if h.err != nil {
			h.err = fmt.Errorf("save: %w", h.err)
			return h.err
		}
	}
	return nil
}

// Query retrieves rows from database
func (h *SQLiteHelper) Query(querySql string, args ...any) (dhl.Rows, error) {

	if h.err != nil {
		return nil, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}

	var (
		sqr *sql.Rows
		placeholder,
		schema string
		paraminseq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, schema)
	if h.tx != nil {
		sqr, h.err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, h.err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("query: %w", h.err)
		return nil, h.err
	}
	if sqr == nil {
		h.err = fmt.Errorf("query: %w", dhl.ErrNoConn)
		return nil, h.err
	}
	return NewSQLiteRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (h *SQLiteHelper) QueryArray(querySql string, out any, args ...any) error {
	if h.err != nil {
		return h.err
	}

	var (
		sqr *sql.Rows
		placeholder,
		schema string
		paraminseq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		h.err = fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported)
		return h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("queryarray: %w", dhl.ErrNoConn)
		return h.err
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder)
	// replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.InterpolateTable(querySql, schema)
	if h.tx != nil {
		sqr, h.err = h.tx.QueryContext(h.ctx, querySql, args...)
	} else {
		sqr, h.err = h.conn.QueryContext(h.ctx, querySql, args...)
	}
	if h.err != nil {
		h.err = fmt.Errorf("queryarray: %w", h.err)
		return h.err
	}
	if sqr == nil {
		h.err = fmt.Errorf("queryarray: %w", dhl.ErrNoConn)
		return h.err
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		idx := 0
		if t == nil {
			t = new([]string)
		}
		for sqr.Next() {
			*t = append(*t, "")
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int:
		idx := 0
		if t == nil {
			t = new([]int)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int8:
		idx := 0
		if t == nil {
			t = new([]int8)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int16:
		idx := 0
		if t == nil {
			t = new([]int16)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int32:
		idx := 0
		if t == nil {
			t = new([]int32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]int64:
		idx := 0
		if t == nil {
			t = new([]int64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]bool:
		idx := 0
		if t == nil {
			t = new([]bool)
		}
		for sqr.Next() {
			*t = append(*t, false)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]float32:
		idx := 0
		if t == nil {
			t = new([]float32)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]float64:
		idx := 0
		if t == nil {
			t = new([]float64)
		}
		for sqr.Next() {
			*t = append(*t, 0)
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	case *[]time.Time:
		idx := 0
		if t == nil {
			t = new([]time.Time)
		}
		for sqr.Next() {
			*t = append(*t, time.Time{})
			if h.err = sqr.Scan(&(*t)[idx]); h.err != nil {
				h.err = fmt.Errorf("queryarray: %w", h.err)
				return h.err
			}
			idx++
		}
		if h.err = sqr.Err(); h.err != nil {
			h.err = fmt.Errorf("queryarray: %w", h.err)
			return h.err
		}
		_ = t
	}
	return nil
}

// QueryRow retrieves a single row from a query
func (h *SQLiteHelper) QueryRow(querySql string, args ...any) dhl.Row {
	if h.err != nil {
		return nil
	}
	if h.conn == nil {
		h.err = fmt.Errorf("queryrow: %w", dhl.ErrNoConn)
		return nil
	}
	var (
		placeholder,
		schema string
		paraminseq bool
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	if h.tx != nil {
		return NewSQLServerRow(h.tx.QueryRowContext(h.ctx, querySql, args...))
	}
	return NewSQLServerRow(h.conn.QueryRowContext(h.ctx, querySql, args...))
}

// Exec executes data manipulation command and returns the number of affected rows
func (h *SQLiteHelper) Exec(querySql string, args ...any) (int64, error) {
	if h.err != nil {
		return 0, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("exec: %w", dhl.ErrNoConn)
		return 0, h.err
	}
	var (
		placeholder,
		schema string
		paraminseq bool
		ra         int64
		sq         sql.Result
	)

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}
	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.InterpolateTable(dhl.ReplaceQueryParamMarker(querySql, paraminseq, placeholder), schema)
	if h.tx != nil {
		sq, h.err = h.tx.ExecContext(h.ctx, querySql, args...)
		if h.err != nil {
			if !errors.Is(h.err, sql.ErrTxDone) {
				h.err = fmt.Errorf("exec: %w", h.err)
				return 0, h.err
			}
			h.err = nil
		}
		ra, _ = sq.RowsAffected()
		return ra, nil
	}
	sq, h.err = h.conn.ExecContext(h.ctx, querySql, args...)
	if h.err != nil {
		h.err = fmt.Errorf("exec: %w", h.err)
		return 0, h.err
	}
	ra, _ = sq.RowsAffected()
	return ra, nil
}

// Exists checks if a record exist
func (h *SQLiteHelper) Exists(sqlWithParams string, args ...any) (bool, error) {

	var (
		cnt int
		sql,
		placeholder,
		schema string
		paraminseq bool
	)

	if h.err != nil {
		return false, h.err
	}
	if h.conn == nil {
		return false, nil
	}

	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, paraminseq, placeholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, schema)
	if strings.HasSuffix(sqlWithParams, `;`) {
		h.err = errors.New(`semicolons are not allowed at the end of this query`)
		return false, h.err
	}
	sql = `SELECT EXISTS(SELECT 1 FROM ` + sqlWithParams + ` LIMIT 1);`
	if h.tx != nil {
		h.err = h.tx.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
		if h.err != nil {
			if !errors.Is(h.err, dhl.ErrNoRows) {
				h.err = fmt.Errorf("exists: %w", h.err)
				return false, h.err
			}
			h.err = nil
		}
		return false, nil
	}
	h.err = h.conn.QueryRowContext(h.ctx, sql, args...).Scan(&cnt)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			h.err = fmt.Errorf("exists: %w", h.err)
			return false, h.err
		}
		h.err = nil
	}
	return cnt == 1, nil
}

// Next gets the next serial number
func (h *SQLiteHelper) Next(serial string, next *int64) error {

	var (
		sql  string
		affr int64
	)
	if h.err != nil {
		return h.err
	}
	if next == nil {
		h.err = fmt.Errorf("next: %w", dhl.ErrVarMustBeInit)
		return h.err
	}
	// if the database config has set a sequence generator, this will use it
	sg := h.dbi.SequenceGenerator
	if sg == nil {
		h.err = fmt.Errorf("next: no sequence generator has been configured")
		return h.err
	}

	if sg.NamePlaceHolder == "" {
		h.err = errors.New(`next: name place holder should be provided. ` +
			`Set name place holder in {placeholder} format. ` +
			`Place holder name should also be present in the upsert or select query`)
		return h.err
	}
	if sg.ResultQuery == "" {
		h.err = errors.New(`next: result query must be provided`)
		return h.err
	}
	// Upsert is usually an insert or an update, so we execute it.
	// It is optional when all queries are set in the result query.
	// affr (affected rows) must be at least 1 to proceed
	affr = 1
	if sg.UpsertQuery != "" {
		sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
		affr, h.err = h.Exec(sql)
		if h.err != nil {
			h.err = fmt.Errorf("next: %w", h.err)
			return h.err
		}
	}
	// in the event that the upsert alters the affr variable to 0, we return an error
	if affr == 0 {
		h.err = errors.New(`next: upsert query did not insert or update any records`)
		return h.err
	}
	// result query needs a single scalar value to be returned
	sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
	h.err = h.QueryRow(sql).Scan(next)
	if h.err != nil {
		h.err = fmt.Errorf("next: %w", h.err)
		return h.err
	}
	return nil
}

// VerifyWithin a set of validation expression against the underlying database table
func (h *SQLiteHelper) VerifyWithin(tableName string, values []dhl.VerifyExpression) (Valid bool, Error error) {
	if h.err != nil {
		return false, h.err
	}
	if h.conn == nil {
		h.err = fmt.Errorf("verify: %w", dhl.ErrNoConn)
		return false, h.err
	}

	var (
		i, exists int
		andstr,
		placeholder,
		schema,
		ph string
		paraminseq bool
	)

	args := make([]any, 0)
	placeholder = "?"
	if h.dbi.ParameterPlaceHolder != nil && *h.dbi.ParameterPlaceHolder != "" {
		placeholder = *h.dbi.ParameterPlaceHolder
	}
	if h.dbi.ParameterInSequence != nil {
		paraminseq = *h.dbi.ParameterInSequence
	}
	if h.dbi.Schema != nil && *h.dbi.Schema != "" {
		schema = *h.dbi.Schema
	}
	tableNameWithParameters := tableName
	if len(values) > 0 {
		tableNameWithParameters += ` WHERE `
	}
	ph = placeholder
	for _, v := range values {
		if isInterfaceNil(v.Value) {
			v.Operator = " IS NULL"
			ph = ""
		} else {
			// If there is no operator, we default to "="
			if v.Operator == "" {
				v.Operator = "="
			}
			if paraminseq {
				ph = placeholder + strconv.Itoa(i+1)
			}
			args = append(args, v.Value)
			i++
		}
		tableNameWithParameters += andstr + v.Name + v.Operator + ph
		andstr = " AND "
	}

	sql := dhl.InterpolateTable(`SELECT EXISTS(SELECT 1 FROM `+tableNameWithParameters+` LIMIT 1);`, schema)
	h.err = h.QueryRow(sql, args...).Scan(&exists)
	if h.err != nil {
		if !errors.Is(h.err, dhl.ErrNoRows) {
			h.err = fmt.Errorf("verify: %w", h.err)
			return false, h.err
		}
		h.err = nil
	}
	return exists == 1, nil
}

// Escape a field value (fv) from disruption by single quote
func (h *SQLiteHelper) Escape(fv string) string {
	if len(fv) == 0 {
		return ""
	}

	senc := `'`
	sesc := `\`
	if h.dbi.StringEnclosingChar != nil && *h.dbi.StringEnclosingChar != "" {
		senc = *h.dbi.StringEnclosingChar
	}
	if h.dbi.StringEscapeChar != nil && *h.dbi.StringEscapeChar != "" {
		sesc = *h.dbi.StringEscapeChar
	}
	return strings.ReplaceAll(fv, senc, sesc+sesc)
}

// DatabaseVersion returns database version
func (h *SQLiteHelper) DatabaseVersion() string {
	var (
		version string
	)
	h.err = h.QueryRow(`SELECT sqlite_version();`).Scan(&version)
	if h.err != nil {
		version = h.err.Error()
		h.err = nil
	}
	return version
}

// Now gets the current server date
func (h *SQLiteHelper) Now() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT DATETIME('now', 'localtime');`).Scan(&tm)
	if h.err != nil {
		tm = time.Now()
		h.err = nil
		return &tm
	}
	return &tm
}

// NowUTC gets the current server date in UTC
func (h *SQLiteHelper) NowUTC() *time.Time {
	var tm time.Time
	h.err = h.QueryRow(`SELECT CURRENT_TIMESTAMP;`).Scan(&tm)
	if h.err != nil {
		tm = time.Now().UTC()
		h.err = nil
		return &tm
	}
	return &tm
}

func (h *SQLiteHelper) Ping() error {
	return nil
}
