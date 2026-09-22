package sqtdhlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	ssd "github.com/shopspring/decimal"
)

func copyScannedToDest(dest, src []interface{}) error {
	for i, d := range src {
		switch x := d.(type) {
		case *sql.NullString:
			if x.Valid {
				switch s := dest[i].(type) {
				case *string:
					*s = x.String
				case **string:
					*s = &x.String
				default:
					return errors.New(`unhandled sql.NullString type`)
				}
			}
		case *sql.NullByte:
			if x.Valid {
				switch s := dest[i].(type) {
				case *uint8:
					*s = x.Byte
				case **uint8:
					*s = &x.Byte
				default:
					return errors.New(`unhandled sql.NullByte type`)
				}
			}
		case *sql.NullInt16:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int16:
					*s = x.Int16
				case **int16:
					*s = &x.Int16
				case *uint16:
					*s = uint16(x.Int16)
				case **uint16:
					xs := uint16(x.Int16)
					*s = &xs
				default:
					return errors.New(`unhandled sql.NullInt16 type`)
				}
			}
		case *sql.NullInt32:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int32:
					*s = x.Int32
				case **int32:
					*s = &x.Int32
				case *int:
					*s = int(x.Int32)
				case **int:
					ic := int(x.Int32)
					*s = &ic
				default:
					return errors.New(`unhandled sql.NullInt32 type`)
				}
			}
		case *sql.NullInt64:
			if x.Valid {
				switch s := dest[i].(type) {
				case *int64:
					*s = x.Int64
				case **int64:
					*s = &x.Int64
				default:
					return errors.New(`unhandled sql.NullInt64 type`)
				}
			}
		case *sql.NullFloat64:
			if x.Valid {
				switch s := dest[i].(type) {
				case *float64:
					*s = x.Float64
				case **float64:
					*s = &x.Float64
				case *float32:
					*s = float32(x.Float64)
				case **float32:
					xs := float32(x.Float64)
					*s = &xs
				default:
					return errors.New(`unhandled sql.NullFloat64 type`)
				}
			}
		case *sql.NullBool:
			if x.Valid {
				switch s := dest[i].(type) {
				case *bool:
					*s = x.Bool
				case **bool:
					*s = &x.Bool
				default:
					return errors.New(`unhandled sql.NullBool type`)
				}
			}
		case *sql.NullTime:
			if x.Valid {
				switch s := dest[i].(type) {
				case *time.Time:
					*s = x.Time
				case **time.Time:
					*s = &x.Time
				default:
					return errors.New(`unhandled sql.NullTime type`)
				}
			}
		case *[]byte:
			switch s := dest[i].(type) {
			case *[]byte:
				*s = *x
			case []byte:
				//s = *x
				copy(dest[i].(json.RawMessage), *x)
			case *json.RawMessage:
				*s = *x
			case json.RawMessage:
				s = *x
			case **[]uint8:
				*s = x
			default:
				return errors.New(`unhandled byte type`)
			}
		case *ssd.NullDecimal:
			switch s := dest[i].(type) {
			case *ssd.Decimal:
				*s = x.Decimal
			case **ssd.Decimal:
				*s = &x.Decimal
			default:
				return errors.New(`unhandled shopspring.NullDecimal type`)
			}
		case *sql.RawBytes:
			switch s := dest[i].(type) {
			case *string:
				*s = string(([]byte)(*x))
			case **string:
				**s = string(([]byte)(*x))
			case interface{}:
				xs := string(([]byte)(*x))
				// s = &xs
				dest[i] = xs
			default:
				return errors.New(`unhandled sql.RawBytes type`)
			}
		default:
			return errors.New(`unhandled sql.Null<type>`)
		}
	}
	return nil
}

func prepareDest(dest []interface{}) (destq []interface{}) {

	// create nullable sql destinations
	destq = make([]interface{}, len(dest))

	// return values
	for i, d := range dest {
		switch x := d.(type) {
		case *string, **string:
			destq[i] = &sql.NullString{}
		case *int, *int8, *int32, *uint, *uint32,
			**int, **int8, **int32, **uint, **uint32:
			destq[i] = &sql.NullInt32{}
		case *int16, *uint16, **int16, **uint16:
			destq[i] = &sql.NullInt16{}
		case *int64, *uint64,
			**int64, **uint64:
			destq[i] = &sql.NullInt64{}
		case *float32, *float64, **float32, **float64:
			destq[i] = &sql.NullFloat64{}
		case *bool, **bool:
			destq[i] = &sql.NullBool{}
		case *time.Time, **time.Time:
			destq[i] = &sql.NullTime{}
		case []uint8, *[]uint8, **[]uint8, *json.RawMessage, json.RawMessage:
			destq[i] = &[]byte{}
		case *ssd.Decimal, **ssd.Decimal:
			destq[i] = &ssd.NullDecimal{}
		case *uint8, **uint8:
			destq[i] = &sql.NullByte{}
		case interface{}, *interface{}:
			destq[i] = &sql.RawBytes{}
		default:
			log.Fatal("Unhandled data type: " + reflect.TypeOf(x).Name())
		}
	}

	return
}

// isInterfaceNil checks if an interface is nil
func isInterfaceNil(i any) bool {
	if i == nil {
		return true
	}
	iv := reflect.ValueOf(i)
	if !iv.IsValid() {
		return true
	}
	switch iv.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Func, reflect.Interface:
		return iv.IsNil()
	default:
		return false
	}
}

func handlePanic(err *error) {
	if r := recover(); r != nil {
		if err != nil {
			*err = fmt.Errorf("recovered: %v", r)
		}
	}
}
