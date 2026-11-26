package store

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
void resurgo_set_progress(sqlite3 *db, int every, uintptr_t handle);
int resurgo_statements(sqlite3 *db, const char *sql);
*/
import "C"

import (
	"errors"
	"reflect"
	"runtime/cgo"
	"unsafe"

	"github.com/mattn/go-sqlite3"
)

// progress counts the callbacks of SQLite's progress handler and, past
// limit (0: none), aborts the statement with SQLITE_INTERRUPT (STO-7)
type progress struct {
	calls int64
	limit int64
}

//export resurgoProgress
func resurgoProgress(handle C.uintptr_t) C.int {
	p := cgo.Handle(handle).Value().(*progress) //nolint:forcetypeassert // only progress handles are registered
	p.calls++
	if p.limit > 0 && p.calls > p.limit {
		return 1
	}
	return 0
}

// setProgress installs the handler on the connection, called every n
// virtual machine steps; the returned function removes it
func setProgress(conn *sqlite3.SQLiteConn, every int, p *progress) (func(), error) {
	db, err := rawDB(conn)
	if err != nil {
		return nil, err
	}
	h := cgo.NewHandle(p)
	C.resurgo_set_progress(db, C.int(every), C.uintptr_t(h))
	return func() {
		C.resurgo_set_progress(db, 0, 0)
		h.Delete()
	}, nil
}

// statements counts the statements in sql, up to 2 (-1: does not prepare,
// the error comes when it runs)
func statements(conn *sqlite3.SQLiteConn, sql string) (int, error) {
	db, err := rawDB(conn)
	if err != nil {
		return 0, err
	}
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return int(C.resurgo_statements(db, cs)), nil
}

// rawDB reads the driver's unexported *C.sqlite3: it depends on the layout
// of the pinned driver, checked by the spike and the tests, and fails
// rather than guesses if that changes
func rawDB(conn *sqlite3.SQLiteConn) (*C.sqlite3, error) {
	f := reflect.ValueOf(conn).Elem().FieldByName("db")
	if !f.IsValid() || f.Kind() != reflect.Pointer || f.Type().Elem().Name() != "_Ctype_struct_sqlite3" {
		return nil, errors.New("mattn/go-sqlite3: SQLiteConn.db is no longer a *C.sqlite3")
	}
	p := *(*unsafe.Pointer)(unsafe.Pointer(f.UnsafeAddr())) //nolint:gosec // see above
	if p == nil {
		return nil, errors.New("mattn/go-sqlite3: connection closed")
	}
	return (*C.sqlite3)(p), nil
}
