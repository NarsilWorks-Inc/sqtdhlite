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

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	_ "modernc.org/sqlite"
)

// SQLiteHelper implements DataHelperLite for SQLite
type SQLiteHelper struct {
	hndl       dhl.DataHelperHandle
	ctx        context.Context
	tx         *sql.Tx
	trCnt      uint16
	rw         sync.RWMutex
	finalizeMu sync.Mutex
	err        error
	rollbackTriggered,
	committed bool
	frames    []bool
	manualCnt uint16 // manual-mode nesting
}

func init() {
	dhl.SetHelper(`sqtdhlite`, &SQLiteHelper{})
	dhl.SetErrNoRows(sql.ErrNoRows)
}

// NewHelper instantiates new helper
func (h *SQLiteHelper) NewHelper() dhl.DataHelperLite {
	return &SQLiteHelper{}
}

// Acquire sets all queries to a new context from pool.
func (dh *SQLiteHelper) Acquire(ctx context.Context, h dhl.DataHelperHandle) error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	dh.ctx = ctx
	dh.hndl = h
	return nil
}

// Begin a transaction. If there is an existing transaction, begin is ignored
func (dh *SQLiteHelper) Begin() error {
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	if dh.manualCnt > 0 {
		dh.err = errors.New("begin: cannot mix Begin() with BeginManually() in the same transaction")
		return dh.err
	}
	if dh.tx == nil {
		var err error
		dh.tx, err = dh.hndl.DB().BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin: %w", err)
			return dh.err
		}
	}
	// Increment transaction count
	dh.trCnt++
	dh.committed = false         // ✅ Reset commit state
	dh.rollbackTriggered = false // ✅ Reset rollback state

	// Track this scope’s deferred rollback
	dh.frames = append(dh.frames, true) // armed
	return nil
}

// BeginManually begins a transaction that does not support deferred rollback.
func (dh *SQLiteHelper) BeginManually() error {
	dh.rw.Lock()
	defer dh.rw.Unlock()
	if dh.err != nil {
		return dh.err
	}
	if dh.hndl == nil {
		dh.err = fmt.Errorf("begin-manually: %w", dhl.ErrHandleNotSet)
		return dh.err
	}
	if dh.trCnt > 0 || len(dh.frames) > 0 {
		dh.err = errors.New("begin-manually: cannot mix BeginManually() with Begin() in the same transaction")
		return dh.err
	}
	if dh.tx == nil {
		var err error
		dh.tx, err = dh.hndl.DB().BeginTx(dh.ctx, nil)
		if err != nil {
			dh.err = fmt.Errorf("begin-manually: %w", err)
			return dh.err
		}
	}

	// Increment transaction count
	dh.trCnt++
	dh.manualCnt++
	dh.committed = false         // Reset commit state
	dh.rollbackTriggered = false // Reset rollback state
	return nil
}

func (dh *SQLiteHelper) Commit() error {
	dh.rw.RLock()
	tx, trCnt, committed, rb, herr, hndl, manualCnt := dh.tx, dh.trCnt, dh.committed, dh.rollbackTriggered, dh.err, dh.hndl, dh.manualCnt
	dh.rw.RUnlock()
	if tx == nil || trCnt == 0 || rb || committed {
		return nil
	}
	if herr != nil {
		return dh.Rollback()
	}

	// Manual mode
	if manualCnt > 0 {
		if hndl == nil {
			dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
			return dh.err
		}
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		// outermost manual: real commit
		dh.finalizeMu.Lock()
		err := tx.Commit()
		dh.finalizeMu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			dh.setDHErr(fmt.Errorf("commit: %w", err))
			return dh.err
		}

		dh.rw.Lock()
		dh.manualCnt = 0
		dh.tx = nil
		// Treat ErrTxDone as success (idempotent commit)
		if err == nil || errors.Is(err, sql.ErrTxDone) {
			dh.committed = true
			dh.err = nil
		} else {
			dh.committed = false
		}
		dh.rollbackTriggered = false
		dh.frames = nil
		dh.trCnt = 0
		dh.rw.Unlock()
		return nil
	}

	// Deferred mode
	// If the transaction is not the outermost transaction, reduce transaction count.
	if trCnt > 1 {
		dh.rw.Lock()
		if n := len(dh.frames); n > 0 {
			dh.frames[n-1] = false // DISARMED
		}
		dh.trCnt--
		dh.rw.Unlock()
		return nil
	}

	// Ensure DB, connection, and transaction are valid before committing
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("commit: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	// Serialize finalization
	// Commit the outermost transaction
	dh.finalizeMu.Lock()
	err := tx.Commit()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("commit: %w", err))
		return dh.err
	}

	// Mark committed, set transaction to nil and set rollback flag to false
	dh.rw.Lock()
	dh.trCnt = 0
	if err == nil || errors.Is(err, sql.ErrTxDone) {
		dh.committed = true
		dh.err = nil
	}
	dh.tx = nil
	dh.rollbackTriggered = false
	dh.frames = nil
	dh.rw.Unlock()
	return nil
}

func (dh *SQLiteHelper) Rollback() error {
	dh.rw.RLock()
	tx, trCnt, manualCnt, committed, herr := dh.tx, dh.trCnt, dh.manualCnt, dh.committed, dh.err
	dh.rw.RUnlock()

	if tx == nil || committed {
		return nil
	}

	// Manual mode
	if manualCnt > 0 {
		if manualCnt > 1 {
			dh.rw.Lock()
			dh.manualCnt--
			if dh.trCnt > 0 {
				dh.trCnt--
			}
			dh.rw.Unlock()
			return nil
		}
		// outermost manual
		return dh.rollbk()
	}

	// Deferred mode
	// If this scope was committed earlier, its defer no-ops
	dh.rw.Lock()
	if n := len(dh.frames); n > 0 && !dh.frames[n-1] {
		dh.frames = dh.frames[:n-1]
		dh.rw.Unlock()
		return nil
	}
	dh.rw.Unlock()

	if herr != nil {
		return dh.rollbk()
	}

	if trCnt > 1 {
		dh.rw.Lock()
		dh.rollbackTriggered = true
		if n := len(dh.frames); n > 0 {
			dh.frames = dh.frames[:n-1] // pop armed
		}
		if dh.trCnt > 0 {
			dh.trCnt--
		}
		dh.rw.Unlock()
		return nil
	}

	// If this is the outermost transaction, rollback the transaction
	return dh.rollbk()
}

func (dh *SQLiteHelper) rollbk() error {
	dh.rw.RLock()
	tx, hndl := dh.tx, dh.hndl
	dh.rw.RUnlock()
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("rollbk: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	dh.rw.Lock()
	dh.rollbackTriggered = true // 🔧 Mark rollback occurred
	dh.rw.Unlock()

	// serialize finalization
	dh.finalizeMu.Lock()
	err := tx.Rollback()
	dh.finalizeMu.Unlock()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		dh.setDHErr(fmt.Errorf("rollbk: %w", err))
		return dh.err
	}

	// Reset all transaction state after rollback
	dh.rw.Lock()
	defer dh.rw.Unlock()
	dh.tx = nil
	dh.trCnt = 0
	dh.err = nil
	dh.committed = false         // 🔧 Reset flags
	dh.rollbackTriggered = false // 🔧 Reset flags (rollback is done)
	dh.frames = nil              // NEW: clear frames
	return nil
}

// Mark a savepoint
func (dh *SQLiteHelper) Mark(name string) error {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if tx == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("mark: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	if trCnt > 0 {
		_, err := tx.ExecContext(dh.ctx, `SAVEPOINT sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("mark: %w", err))
			return dh.err
		}
	}

	return nil
}

// Discard a savepoint
func (dh *SQLiteHelper) Discard(name string) error {
	dh.rw.RLock()
	tx, herr, trCnt, hndl := dh.tx, dh.err, dh.trCnt, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("discard: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	if trCnt > 0 {
		_, err := tx.ExecContext(dh.ctx, `ROLLBACK TO SAVEPOINT sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("discard: %w", err))
			return dh.err
		}
	}
	return nil
}

// Save a savepoint
func (dh *SQLiteHelper) Save(name string) error {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}
	if tx == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrNoTx))
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("save: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if dh.trCnt > 0 {
		_, err := tx.ExecContext(dh.ctx, `RELEASE TO sp_`+sanitizeName(name)+`;`)
		if err != nil {
			dh.setDHErr(fmt.Errorf("save: %w", err))
			return dh.err
		}
	}
	return nil
}

// Query retrieves rows from database
func (dh *SQLiteHelper) Query(querySql string, args ...any) (dhl.Rows, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return nil, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("query: %w", dhl.ErrHandleNotSet))
		return nil, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	var (
		sqr *sql.Rows
		err error
	)
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("query: %w", err))
		return nil, dh.err
	}

	return NewSQLiteRows(sqr), nil
}

// QueryArray puts the single column result to an output array
func (dh *SQLiteHelper) QueryArray(querySql string, out any, args ...any) error {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrHandleNotSet))
		return dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()
	switch out.(type) {
	case *[]string, *[]int, *[]int8, *[]int16, *[]int32, *[]int64, *[]bool, *[]float32, *[]float64:
	case *[]time.Time:
	default:
		dh.setDHErr(fmt.Errorf("queryarray: %w", dhl.ErrArrayTypeNotSupported))
		return dh.err
	}

	// Replace question mark (?) parameter with configured query parameter, if there are any
	// Replace tables meant for interpolation {table} for putting the schema
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)
	var (
		sqr *sql.Rows
		err error
	)
	if tx != nil {
		sqr, err = tx.QueryContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().QueryContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("queryarray: %w", err))
		return dh.err
	}
	defer sqr.Close()

	switch t := out.(type) {
	case *[]string:
		var v string
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.err = fmt.Errorf("queryarray: %w", dh.err)
			return dh.err
		}
		_ = t
	case *[]int:
		var v int
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int8:
		var v int8
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int16:
		var v int16
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int32:
		var v int32
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]int64:
		var v int64
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]bool:
		var v bool
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float32:
		var v float32
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]float64:
		var v float64
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	case *[]time.Time:
		var v time.Time
		for sqr.Next() {
			if dh.err = sqr.Scan(&v); dh.err != nil {
				dh.setDHErr(fmt.Errorf("queryarray: %w", err))
				return dh.err
			}
			*t = append(*t, v)
		}
		if dh.err = sqr.Err(); dh.err != nil {
			dh.setDHErr(fmt.Errorf("queryarray: %w", err))
			return dh.err
		}
		_ = t
	}
	return nil
}

// QueryRow retrieves a single row from a query
func (dh *SQLiteHelper) QueryRow(querySql string, args ...any) dhl.Row {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()

	if herr != nil {
		return NewSQLiteRow(nil)
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("queryrow: %w", dhl.ErrHandleNotSet))
		return NewSQLiteRow(nil)
	}
	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	if tx != nil {
		return NewSQLiteRow(tx.QueryRowContext(dh.ctx, querySql, args...))
	} else {
		return NewSQLiteRow(hndl.DB().QueryRowContext(dh.ctx, querySql, args...))
	}
}

// Exec executes data manipulation command and returns the number of affected rows
func (dh *SQLiteHelper) Exec(querySql string, args ...any) (int64, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return 0, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exec: %w", dhl.ErrHandleNotSet))
		return 0, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	querySql = dhl.ReplaceQueryParamMarker(querySql, paramInSeq, placeholder)
	querySql = dhl.InterpolateTable(querySql, schema)

	var (
		sqr sql.Result
		err error
	)
	if tx != nil {
		sqr, err = tx.ExecContext(dh.ctx, querySql, args...)
	} else {
		sqr, err = hndl.DB().ExecContext(dh.ctx, querySql, args...)
	}
	if err != nil {
		dh.setDHErr(fmt.Errorf("exec: %w", err))
		return 0, dh.err
	}
	ra, _ := sqr.RowsAffected()

	return ra, nil
}

// Exists checks if a record exist
func (dh *SQLiteHelper) Exists(sqlWithParams string, args ...any) (bool, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}

	if hndl == nil {
		dh.setDHErr(fmt.Errorf("exists: %w", dhl.ErrHandleNotSet))
		return false, dh.err
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sqlWithParams = dhl.ReplaceQueryParamMarker(sqlWithParams, paramInSeq, placeholder)
	sqlWithParams = dhl.InterpolateTable(sqlWithParams, schema)
	sqlWithParams = strings.TrimSpace(sqlWithParams)
	if strings.HasSuffix(sqlWithParams, `;`) {
		dh.setDHErr(errors.New(`semicolons are not allowed at the end of this query`))
		return false, dh.err
	}

	var b strings.Builder
	b.Grow(len(sqlWithParams) + 20)
	b.WriteString("SELECT EXISTS (SELECT 1 FROM ")
	b.WriteString(sqlWithParams)
	b.WriteString(` LIMIT 1);`)
	sqlq := b.String()

	var exists bool
	if tx != nil {
		err := tx.QueryRowContext(dh.ctx, sqlq, args...).Scan(&exists)
		if err != nil {
			if !errors.Is(err, dhl.ErrNoRows) {
				dh.setDHErr(fmt.Errorf("exists: %w", err))
				return false, dh.err
			}
		}
		return exists, nil
	}
	err := hndl.DB().QueryRowContext(dh.ctx, sqlq, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.setDHErr(fmt.Errorf("exists: %w", err))
			return false, dh.err
		}
	}
	return exists, nil
}

// Next gets the next serial number
func (dh *SQLiteHelper) Next(serial string, next *int64) error {
	dh.rw.RLock()
	herr, hndl := dh.err, dh.hndl
	dh.rw.RUnlock()
	var (
		sql  string
		affr int64
	)
	if herr != nil {
		return dh.err
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrHandleNotSet))
		return dh.err
	}
	if next == nil {
		dh.setDHErr(fmt.Errorf("next: %w", dhl.ErrVarMustBeInit))
		return dh.err
	}
	di := dh.hndl.DI()
	if di == nil {
		dh.setDHErr(errors.New("next: database info not configured"))
		return dh.err
	}

	// if the database config has set a sequence generator, this will use it
	sg := di.SequenceGenerator
	if sg != nil {
		if sg.NamePlaceHolder == "" {
			dh.setDHErr(errors.New(`next: name place holder should be provided. ` +
				`Set name place holder in {placeholder} format. ` +
				`Place holder name should also be present in the upsert or select query`))
			return dh.err
		}
		if sg.ResultQuery == "" {
			dh.setDHErr(errors.New("next: result query must be provided"))
			return dh.err
		}
		// Upsert is usually an insert or an update, so we execute it.
		// It is optional when all queries are set in the result query.
		// affr (affected rows) must be at least 1 to proceed
		affr = 1
		if sg.UpsertQuery != "" {
			sql = strings.ReplaceAll(sg.UpsertQuery, sg.NamePlaceHolder, serial)
			affr, dh.err = dh.Exec(sql)
			if dh.err != nil {
				dh.setDHErr(fmt.Errorf("next: %w", dh.err))
				return dh.err
			}
		}
	}

	// in the event that the upsert alters the affr variable to 0, we return an error
	if affr == 0 {
		dh.setDHErr(errors.New("next: upsert query did not insert or update any records"))
		return dh.err
	}
	// result query needs a single scalar value to be returned
	sql = strings.ReplaceAll(sg.ResultQuery, sg.NamePlaceHolder, serial)
	dh.err = dh.QueryRow(sql).Scan(next)
	if dh.err != nil {
		dh.setDHErr(fmt.Errorf("next: %w", dh.err))
		return dh.err
	}
	return nil
}

// ExistsExt a set of validation expression against the underlying database table
func (dh *SQLiteHelper) ExistsExt(tableName string, values []dhl.ColumnFilter) (Valid bool, Error error) {
	dh.rw.RLock()
	herr, hndl := dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return false, herr
	}
	if hndl == nil {
		dh.setDHErr(fmt.Errorf("existsext: %w", dhl.ErrHandleNotSet))
		return false, dh.err
	}

	var (
		andstr, sqlq,
		ph string
		exists bool
		i      int
	)

	args := make([]any, 0)

	placeholder, paraminseq, schema := dh.getParamDataInfo()

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

	tableNameWithParameters = strings.TrimSpace(tableNameWithParameters)
	if strings.HasSuffix(tableNameWithParameters, `;`) {
		tableNameWithParameters, _ = strings.CutSuffix(tableNameWithParameters, `;`)
	}

	sqlq = dhl.InterpolateTable(`SELECT EXISTS (SELECT 1 FROM `+tableNameWithParameters+` LIMIT 1);`, schema)
	err := dh.QueryRow(sqlq, args...).Scan(&exists)
	if err != nil {
		if !errors.Is(err, dhl.ErrNoRows) {
			dh.rw.Lock()
			dh.err = fmt.Errorf("existsext: %w", err)
			dh.rw.Unlock()
			return false, dh.err
		}
		return false, nil
	}

	return exists, nil
}

// Escape a field value (fv) from disruption by single quote
func (dh *SQLiteHelper) Escape(fv string) string {
	if fv == "" {
		return ""
	}
	return strings.ReplaceAll(fv, `'`, `\'`)
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

// UpsertReturning inserts a row into the table.
// If a conflict occurs on the specified unique columns:
//
//   - If updateColumns is empty, the existing row is returned unchanged
//   - If updateColumns is provided, the existing row is updated using EXCLUDED values
//
// Parameters:
//   - insertColumns - columns in the INSERT. All NOT NULL columns without defaults must be included.
//   - uniqueColumns - columns defining the conflict target
//   - updateColumns -
//     1. empty or nil → do not modify existing row on conflict
//     2. non-empty → DO UPDATE SET col = EXCLUDED.col
//   - returnColumns - columns to return
//   - args - values for insertColumns, in order
//
// The method always returns the resulting row.
func (dh *SQLiteHelper) UpsertReturning(
	tableName string,
	insertColumns []string,
	uniqueColumns []string,
	updateColumns []string,
	returnColumns []string,
	args ...any,
) (dhl.Row, error) {
	dh.rw.RLock()
	tx, herr, hndl := dh.tx, dh.err, dh.hndl
	dh.rw.RUnlock()
	if herr != nil {
		return NewSQLiteRow(nil), herr
	}
	if hndl == nil {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %w", dhl.ErrHandleNotSet)
	}
	if tableName == "" {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "table name not set")
	}
	if len(insertColumns) == 0 {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns needs to be set")
	}
	if len(uniqueColumns) == 0 {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "unique columns needs to be set")
	}
	if len(returnColumns) == 0 {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "return columns needs to be set")
	}
	if len(insertColumns) != len(args) {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "insert columns count and arguments mismatch")
	}
	if len(updateColumns) > 0 {
		for _, updCol := range updateColumns {
			found := false
			for _, insCol := range insertColumns {
				if strings.EqualFold(insCol, updCol) {
					found = true
					break
				}
			}
			if !found {
				return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %s", "update columns does not exist in insert columns")
			}
		}
	}
	db := hndl.DB()
	if db == nil {
		return NewSQLiteRow(nil), fmt.Errorf("upsertreturning: %w", dhl.ErrHandleDBNotSet)
	}

	// Build query
	cma := ""
	sql := "INSERT INTO " + tableName + " (" + strings.Join(insertColumns, ",")
	sql += ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(insertColumns)), ",") + ")\n"
	sql += "ON CONFLICT (" + strings.Join(uniqueColumns, ",") + ")\n"
	if len(updateColumns) == 0 {
		sql += "DO NOTHING"
	} else {
		sql += "DO UPDATE SET "
		for _, updCol := range updateColumns {
			sql += cma + updCol + "=EXCLUDED." + updCol
			cma = ","
		}
	}
	sql += "\n"
	sql += "RETURNING "
	cma = ""
	for _, retCol := range returnColumns {
		sql += cma + retCol
		cma = ","
	}

	placeholder, paramInSeq, schema := dh.getParamDataInfo()

	// replace question mark (?) parameter with configured query parameter, if there are any
	sql = dhl.ReplaceQueryParamMarker(sql, paramInSeq, placeholder)
	sql = dhl.InterpolateTable(sql, schema)

	defer handlePanic(nil)
	if tx != nil {
		return NewSQLiteRow(tx.QueryRowContext(dh.ctx, sql, args...)), nil
	} else {
		return NewSQLiteRow(db.QueryRowContext(dh.ctx, sql, args...)), nil
	}
}

// VendorStatement returns a vendor-specific statement or query when present. Returns an empty string if not present
func (dh *SQLiteHelper) VendorStatement(key string) string {
	return ""
}

// VendorStatements Lists the vendor-specific statements implemented in a helper
func (dh *SQLiteHelper) VendorStatements() []string {
	return []string{}
}

func (dh *SQLiteHelper) getParamDataInfo() (ph string, pis bool, sch string) {
	dh.rw.RLock()
	h := dh.hndl
	dh.rw.RUnlock()
	ph = "?"
	sch = ""
	if h == nil || h.DI() == nil {
		return
	}
	if h.DI().ParameterPlaceHolder != nil && *h.DI().ParameterPlaceHolder != "" {
		ph = *h.DI().ParameterPlaceHolder
	}
	if h.DI().ParameterInSequence != nil {
		pis = *h.DI().ParameterInSequence
	}
	if h.DI().Schema != nil && *h.DI().Schema != "" {
		sch = *h.DI().Schema
	}
	return
}

func sanitizeName(s string) string {
	// replace non [A-Za-z0-9_] with _
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			b[i] = c
		} else {
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return "sp"
	}
	return string(b)
}

func (h *SQLiteHelper) setDHErr(err error) {
	h.rw.Lock()
	h.err = err
	h.rw.Unlock()
}
