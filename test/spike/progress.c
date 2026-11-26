// The progress-handler shim of EXT-S-2: mattn/go-sqlite3 does not expose
// sqlite3_progress_handler, so it is called here on the SQLite amalgamation
// that the driver bundles (its SQLITE_API symbols are linked into the same
// binary). The prototype matches sqlite3.h of the pinned driver.
#include <stdint.h>
#include "_cgo_export.h"

typedef struct sqlite3 sqlite3;
extern void sqlite3_progress_handler(sqlite3 *, int, int (*)(void *), void *);

static int trampoline(void *handle) { return spikeProgress((uintptr_t)handle); }

void spike_set_progress(sqlite3 *db, int every, uintptr_t handle) {
	sqlite3_progress_handler(db, every, handle ? trampoline : 0, (void *)handle);
}
