package sqtdhlite

import "reflect"

type Column struct {
	name    string
	dbtname string
	scntyp  reflect.Type
}

func (c Column) Name() string {
	return c.name
}

func (c Column) DatabaseTypeName() string {
	return c.dbtname
}

func (c Column) ScanType() reflect.Type {
	return c.scntyp
}
