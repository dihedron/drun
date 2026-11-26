package store

import (
	"time"

	"github.com/dihedron/resurgo/internal/fsm/fsmpb"
	"github.com/mattn/go-sqlite3"
)

// toValue converts a Go value from the driver into a fsmpb.Value
func toValue(v any) *fsmpb.Value {
	switch x := v.(type) {
	case nil:
		return &fsmpb.Value{Value: &fsmpb.Value_Null{Null: true}}
	case int64:
		return &fsmpb.Value{Value: &fsmpb.Value_Integer{Integer: x}}
	case int:
		return &fsmpb.Value{Value: &fsmpb.Value_Integer{Integer: int64(x)}}
	case float64:
		return &fsmpb.Value{Value: &fsmpb.Value_Real{Real: x}}
	case bool:
		return &fsmpb.Value{Value: &fsmpb.Value_Boolean{Boolean: x}}
	case string:
		return &fsmpb.Value{Value: &fsmpb.Value_Text{Text: x}}
	case []byte:
		return &fsmpb.Value{Value: &fsmpb.Value_Blob{Blob: x}}
	case time.Time:
		// the driver parses columns declared DATE, DATETIME or TIMESTAMP:
		// give them back as text, in its first format
		return &fsmpb.Value{Value: &fsmpb.Value_Text{Text: x.Format(sqlite3.SQLiteTimestampFormats[0])}}
	default:
		return &fsmpb.Value{Value: &fsmpb.Value_Null{Null: true}}
	}
}

// fromValue converts a fsmpb.Value into a statement argument
func fromValue(v *fsmpb.Value) any {
	switch x := v.GetValue().(type) {
	case *fsmpb.Value_Integer:
		return x.Integer
	case *fsmpb.Value_Real:
		return x.Real
	case *fsmpb.Value_Text:
		return x.Text
	case *fsmpb.Value_Blob:
		return x.Blob
	case *fsmpb.Value_Boolean:
		if x.Boolean {
			return int64(1)
		}
		return int64(0)
	default:
		return nil
	}
}

// size is what a value counts towards a result's bound (STO-9)
func size(v *fsmpb.Value) int {
	switch x := v.GetValue().(type) {
	case *fsmpb.Value_Text:
		return len(x.Text)
	case *fsmpb.Value_Blob:
		return len(x.Blob)
	default:
		return 8
	}
}
