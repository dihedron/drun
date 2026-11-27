// The parts of the SQLite C API that mattn/go-sqlite3 does not expose,
// called on the amalgamation the driver bundles (its SQLITE_API symbols are
// linked into the same binary); the prototypes match its sqlite3.h.
// Verified by the spike (EXT-S-2).
#include <stdint.h>
#include <stddef.h>
#include "_cgo_export.h"

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
extern void sqlite3_progress_handler(sqlite3 *, int, int (*)(void *), void *);
extern int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
extern int sqlite3_finalize(sqlite3_stmt *);
extern int sqlite3_complete(const char *);

int drun_complete(const char *sql) { return sqlite3_complete(sql); }

static int trampoline(void *handle) { return drunProgress((uintptr_t)handle); }

void drun_set_progress(sqlite3 *db, int every, uintptr_t handle) {
	sqlite3_progress_handler(db, every, handle ? trampoline : 0, (void *)handle);
}

// drun_statements counts the statements in sql, up to 2; whitespace and
// comments are not statements. It returns -1 if one does not prepare.
int drun_statements(sqlite3 *db, const char *sql) {
	int count = 0;
	const char *tail = sql;
	while (tail && *tail && count < 2) {
		sqlite3_stmt *stmt = NULL;
		if (sqlite3_prepare_v2(db, tail, -1, &stmt, &tail) != 0) {
			return -1;
		}
		if (stmt) {
			count++;
			sqlite3_finalize(stmt);
		}
	}
	return count;
}
