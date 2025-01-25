package sqtdhlite

import (
	"context"
	"fmt"
	"testing"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	_ "github.com/denisenkom/go-mssqldb"
	cfg "github.com/eaglebush/config"
	ssd "github.com/shopspring/decimal"
)

const (
	MasterSlaveSQL string = `
		DROP TABLE IF EXISTS SlaveTable1;
		DROP TABLE IF EXISTS SlaveTable2;
		DROP TABLE IF EXISTS MasterTable;
		DROP TABLE IF EXISTS JokeTable;
		DROP TABLE IF EXISTS SequenceTable;

		CREATE TABLE MasterTable (
			ID int,
			Code nvarchar(10),
			[Name] nvarchar(25),
			PRIMARY KEY ([ID] ASC)
		);

		CREATE TABLE SlaveTable1 (
			ParentID int,
			ID int,
			Code nvarchar(10),
			[Name] nvarchar(25),
			PRIMARY KEY ([ID] ASC),
			FOREIGN KEY(ParentID) REFERENCES MasterTable(ID)
		);

		CREATE TABLE SlaveTable2 (
			ParentID int,
			ID int,
			Code nvarchar(10),
			[Name] nvarchar(25),
			PRIMARY KEY ([ID] ASC),
			FOREIGN KEY(ParentID) REFERENCES MasterTable(ID)
		);

		CREATE TABLE JokeTable (
			ID int,
			Code nvarchar(10),
			[Name] nvarchar(25),
			Cost decimal(10,4),
			PRIMARY KEY ([ID] ASC)
		);

		CREATE TABLE SequenceTable (
			sequence_name nvarchar(30),
			next int,
			PRIMARY KEY (sequence_name ASC)
		);

		INSERT INTO MasterTable (ID, Code, [Name]) VALUES (1, 'CODE1', 'Code 1');
		INSERT INTO SlaveTable1 (ID, Code, [Name], ParentID) VALUES (1, 'SLAV1CODE1', 'Slave1 Code 1', 1);
		INSERT INTO SlaveTable1 (ID, Code, [Name], ParentID) VALUES (2, 'SLAV1CODE2', 'Slave1 Code 2', 1);
		INSERT INTO SlaveTable1 (ID, Code, [Name], ParentID) VALUES (3, 'SLAV1CODE3', 'Slave1 Code 3', 1);
		INSERT INTO SlaveTable1 (ID, Code, [Name], ParentID) VALUES (4, 'SLAV1CODE4', 'Slave1 Code 4', 1);
		INSERT INTO SlaveTable1 (ID, Code, [Name], ParentID) VALUES (5, 'SLAV1CODE5', 'Slave1 Code 5', 1);
		INSERT INTO SlaveTable2 (ID, Code, [Name], ParentID) VALUES (6, 'SLAV2CODE1', 'Slave2 Code 1', 1);
		INSERT INTO SlaveTable2 (ID, Code, [Name], ParentID) VALUES (7, 'SLAV2CODE2', 'Slave2 Code 2', 1);
		INSERT INTO SlaveTable2 (ID, Code, [Name], ParentID) VALUES (8, 'SLAV2CODE3', 'Slave2 Code 3', 1);
		INSERT INTO SlaveTable2 (ID, Code, [Name], ParentID) VALUES (9, 'SLAV2CODE4', 'Slave2 Code 4', 1);
		INSERT INTO SlaveTable2 (ID, Code, [Name], ParentID) VALUES (10, 'SLAV2CODE5', 'Slave2 Code 5', 1);
	`
)

func TestLoadData(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	affr, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("%d rows inserted", affr)
}

func TestGetRow(t *testing.T) {
	var (
		err error
		c   dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	type Slave struct {
		ID       int
		ParentID int
		Code     string
		Name     string
	}

	ts := Slave{}

	err = c.QueryRow(`SELECT ID, Code, Name, ParentID FROM SlaveTable2 WHERE ID=?;`, 8).Scan(
		&ts.ID,
		&ts.Code,
		&ts.Name,
		&ts.ParentID)

	if err != nil {
		if err != dhl.ErrNoRows {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Log(err.Error())
	}

	t.Logf("ID: %d, Code: %s, Name: %s, ParentID: %d", ts.ID, ts.Code, ts.Name, ts.ParentID)
}

func TestGetRows(t *testing.T) {

	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}
	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	rows, err := c.Query(`SELECT ID, Code, Name, ParentID FROM SlaveTable1;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	for _, col := range cols {
		t.Log(col.Name(), col.DatabaseTypeName(), col.ScanType())
	}
	ifrows := make([]interface{}, 4)
	brows := make([]string, 4)
	for i := range ifrows {
		ifrows[i] = &brows[i]
	}

	for rows.Next() {
		err = rows.Scan(ifrows...)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Logf("ID: %s, Code: %s, Name: %s, ParentID: %s", brows[0], brows[1], brows[2], brows[3])
	}

	if err = rows.Err(); err != nil {
		t.Log(err.Error())
		return
	}
}

func TestWriteTransactions(t *testing.T) {

	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	c.Begin()
	defer c.Rollback()

	_, err = c.Exec(`DELETE FROM JokeTable;`)
	if err != nil {
		//c.Rollback()
		t.Log(err.Error())
		return
	}

	i := 0

	for {
		if i > 9999 {
			break
		}

		_, err = c.Exec(`
			INSERT INTO JokeTable (ID, Code, Name, Cost)
			VALUES (?, ?, ?, ?);`, i, fmt.Sprintf("CODE%d", i), fmt.Sprintf("Code %d", i), 12.57*float64(i))
		if err != nil {
			//c.Rollback()
			t.Log(err.Error())
			break
		}

		i++
	}
	c.Commit()
}

func TestGetRowsFromJoke(t *testing.T) {

	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}
	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	type Joke struct {
		ID   int
		Code string
		Name string
		Cost ssd.Decimal
	}

	ts := Joke{}

	rows, err := c.Query(`SELECT ID, Code, Name, Cost FROM JokeTable;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&ts.ID, &ts.Code, &ts.Name, &ts.Cost)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Logf("ID: %d, Code: %s, Name: %s, Cost: %v", ts.ID, ts.Code, ts.Name, ts.Cost)
	}

	if err = rows.Err(); err != nil {
		t.Log(err.Error())
		return
	}
}

func TestSequence(t *testing.T) {
	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// pointer, must be initialized to int64

	seq := new(int64)

	err = c.Next(`testsequence`, seq)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	/*
		// non pointer
		var seq int64

		err = c.Next(`testsequence`, &seq)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
	*/

	t.Logf("Sequence for testsequence: %d", *seq)
}

func TestMultipleOpen(t *testing.T) {

	type Slave struct {
		ID       int
		ParentID int
		Code     string
		Name     string
	}

	var repeat = func() {
		var (
			err error
			c   dhl.DataHelperLite
		)

		c, err = dhl.New(nil, `sqtdhlite`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		cf, err := cfg.Load(`config.json`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
		defer c.Close()

		rows, err := c.Query(`SELECT ID, Code, Name, ParentID FROM SlaveTable1;`)
		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
		defer rows.Close()

		ts := Slave{}

		for rows.Next() {
			err = rows.Scan(
				&ts.ID,
				&ts.Code,
				&ts.Name,
				&ts.ParentID)
			if err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}

			t.Logf("ID: %d, Code: %s, Name: %s, ParentID: %d", ts.ID, ts.Code, ts.Name, ts.ParentID)
		}

		if err = rows.Err(); err != nil {
			t.Log(err.Error())
			return
		}

	}

	for i := 0; i < 3; i++ {
		repeat()
	}
}

func TestExists(t *testing.T) {
	var (
		err    error
		exists bool
		c      dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	exists, err = c.Exists(`SlaveTable2 WHERE ID = ?`, 7)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}

func TestQueryArray(t *testing.T) {
	var (
		err error
		c   dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	var arr []string

	err = c.QueryArray(`SELECT code FROM SlaveTable2;`, &arr)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Array: %v", arr)

}

func TestGetBytes(t *testing.T) {
	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`OFFICE`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	var (
		fdat []byte
		fext string
	)

	err = c.QueryRow(`SELECT Resource,
							  FileExtension
						FROM tshApplicationResource
						WHERE ApplicationID = @p1
						AND ResourceID = @p2;`,
		`ArkenstoneTMS`, `E00AFBA42FA84DC5DB2240A7916BF05E15F451F297F5FC86EFC10283866F8CF8`).
		Scan(&fdat, &fext)

	if err != nil {

		if err != dhl.ErrNoRows {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Log(err.Error())
	}
}

func TestExecRowsAffected(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`APPSHUB`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	c.Begin()
	defer c.Rollback()

	affr, err = c.Exec(`UPDATE {useraccount}
						SET activation_code = ?,
							activation_status='PENDING'
						WHERE user_key = ?;`, `1bnSiVeH9qBcxXDn5hAhJQocRmP`, 35)
	if err != nil {
		t.Fatalf(`%s`, err)
	}

	c.Commit()

	t.Logf(`Affected rows %d`, affr)
}

func TestDeferredRollbackNestedTransDeleteNoError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE ID = ?`, 1)
		if err != nil {
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentID = ?`, 1)
		if err != nil {
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentID = ?`, 1)
		if err != nil {
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestDeferredRollbackNestedTransDelete1stQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE ID = ?`, 1)
		if err != nil {
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentID = ?`, 1)
		if err != nil {
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentIDX = ?`, 1)
		if err != nil {
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestDeferredRollbackNestedTransDelete2ndQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE ID = ?`, 1)
		if err != nil {
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentIDX = ?`, 1)
		if err != nil {
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentID = ?`, 1)
		if err != nil {
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestDeferredRollbackNestedTransDelete3rdQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE IDX = ?`, 1)
		if err != nil {
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentID = ?`, 1)
		if err != nil {
			t.Fatalf(`%s`, err)
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		defer dh.Rollback()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentID = ?`, 1)
		if err != nil {
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestManualRollbackNestedTransDelete1stQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE ID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentIDX = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestManualRollbackNestedTransDelete2ndQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE ID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentIDX = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}

func TestManualRollbackNestedTransDelete3rdQueryError(t *testing.T) {
	var (
		err  error
		affr int64
		c    dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `sqtdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	// Drop and Create table
	// Insert data
	// The succeeding statement will attempt to delete
	// the inserted rows
	_, err = c.Exec(MasterSlaveSQL)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Delete master record
	three := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {MasterTable} WHERE IDX = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		dh.Commit()
	}

	two := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable2} WHERE ParentID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		three(dh)
		dh.Commit()
	}

	one := func(dh dhl.DataHelperLite) {
		dh.Begin()
		affr, err = dh.Exec(`DELETE FROM {SlaveTable1} WHERE ParentID = ?`, 1)
		if err != nil {
			dh.Rollback()
			return
		}
		two(dh)
		dh.Commit()
	}

	one(c)

	t.Logf(`Affected rows %d`, affr)
}
