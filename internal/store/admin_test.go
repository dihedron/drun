package store

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dihedron/drun/internal/fsm/fsmpb"
)

func TestSplit(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"SELECT 1", []string{"SELECT 1"}},
		{"CREATE TABLE t (x); INSERT INTO t VALUES (';');\n-- done\n", []string{"CREATE TABLE t (x);", "INSERT INTO t VALUES (';');"}},
		{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET x = 1; END; SELECT 2", []string{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET x = 1; END;", "SELECT 2"}},
		{"  \n-- only a comment\n", nil},
	} {
		if got := Split(tc.sql); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Split(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

func TestInfoAndBackup(t *testing.T) {
	h := newHarness(t)
	if info, err := h.s.Info("svc"); err != nil || info.Exists {
		t.Fatalf("info of a missing store: %+v %v", info, err)
	}
	h.ok("svc", stmt("CREATE TABLE b (x)"), stmt("CREATE TABLE a (x)"), stmt("INSERT INTO a VALUES (1)"))
	info, err := h.s.Info("svc")
	if err != nil || !info.Exists || info.Size == 0 || info.LastIndex != h.index || !reflect.DeepEqual(info.Tables, []string{"a", "b"}) {
		t.Fatalf("info %+v %v", info, err)
	}

	var buf bytes.Buffer
	if _, err := h.s.Backup("svc", &buf); err != nil {
		t.Fatalf("backup: %v", err)
	}
	path := filepath.Join(t.TempDir(), "copy.db")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck // test
	var n int
	if err := db.QueryRow("SELECT count(*) FROM a").Scan(&n); err != nil || n != 1 {
		t.Fatalf("the backup: %d rows, %v", n, err)
	}
	if _, err := h.s.Backup("nope", &buf); err == nil {
		t.Fatal("a backup of a missing store")
	}
}

// --dry-run: the results, nothing applied, no fatal error on a failure
func TestDryRun(t *testing.T) {
	h := newHarness(t)
	h.ok("svc", stmt("CREATE TABLE t (x)"))
	before, _, _ := h.s.Digest("svc")
	res, err := h.s.DryRun(&fsmpb.StoreBatch{Runnable: "svc", Epoch: 9, Statements: []*fsmpb.Statement{
		stmt("INSERT INTO t VALUES (1), (2)"), stmt("SELECT count(*) FROM t"),
	}}, nil)
	if err != nil || res.GetError() != nil || res.GetResults()[1].GetRows()[0].GetValues()[0].GetInteger() != 2 {
		t.Fatalf("dry run: %+v %v", res, err)
	}
	if after, _, _ := h.s.Digest("svc"); after != before || h.count("svc", "t") != 0 {
		t.Fatal("the dry run changed the database")
	}
	res, err = h.s.DryRun(&fsmpb.StoreBatch{Runnable: "svc", Statements: []*fsmpb.Statement{stmt("SELECT random()")}}, nil)
	if err != nil || res.GetError() == nil || !strings.Contains(res.GetError().GetMessage(), "random") {
		t.Fatalf("a refused statement: %+v %v", res, err)
	}
	if len(h.fatals) != 0 {
		t.Fatalf("fatal errors: %v", h.fatals)
	}
	if left, _ := filepath.Glob(filepath.Join(h.dir, ".*")); len(left) != 0 {
		t.Fatalf("temporary files left: %v", left)
	}
}
