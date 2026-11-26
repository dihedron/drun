//go:build cgo

package spike

/*
#include <stdint.h>
typedef struct sqlite3 sqlite3;
void spike_set_progress(sqlite3 *db, int every, uintptr_t handle);
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
// limit (0: none), aborts the statement with SQLITE_INTERRUPT
type progress struct {
	calls int64
	limit int64
}

//export spikeProgress
func spikeProgress(handle C.uintptr_t) C.int {
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
	C.spike_set_progress(db, C.int(every), C.uintptr_t(h))
	return func() {
		C.spike_set_progress(db, 0, 0)
		h.Delete()
	}, nil
}

// rawDB reads the driver's unexported *C.sqlite3: it depends on the layout
// of the pinned driver and fails, rather than guesses, if that changes
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
