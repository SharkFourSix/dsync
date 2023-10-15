package dsync_test

import (
	"database/sql"
	"embed"
	"os"
	"testing"

	"github.com/SharkFourSix/dsync"
	"github.com/SharkFourSix/dsync/sources/mysql"
	"github.com/SharkFourSix/dsync/sources/postgresql"
	"github.com/SharkFourSix/dsync/sources/sqlite"
)

//go:embed resources/migrations
var e embed.FS

func TestPostgresqlDataSource(t *testing.T) {
	dsn := "postgres://postgres:toor@localhost:5433/test-db"
	migrator := dsync.Migrator{OutOfOrder: true}

	ds, err := postgresql.New(dsn, &dsync.Config{
		FileSystem: e,
		Basepath:   "resources/migrations/postgresql",
		TableName:  "dsync_schema_migration",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	err = migrator.Migrate(ds)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestMySqlDataSource(t *testing.T) {
	dsn := "admin:toor@tcp(localhost)/test_db?parseTime=true"
	migrator := dsync.Migrator{OutOfOrder: true}

	ds, err := mysql.New(dsn, &dsync.Config{
		FileSystem: e,
		Basepath:   "resources/migrations/mysql",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	err = migrator.Migrate(ds)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestSqliteDataSource(t *testing.T) {
	dsn := "file:test/test.db?cache=shared&mode=rw"
	migrator := dsync.Migrator{OutOfOrder: true}

	err := os.MkdirAll("./test", 0755)
	if err != nil {
		t.Fatal(err)
	}

	fd, err := os.Create("test/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ds, err := sqlite.New(dsn, &dsync.Config{
		FileSystem: e,
		Basepath:   "resources/migrations/sqlite",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	err = migrator.Migrate(ds)
	if err != nil {
		t.Fatal(err)
		return
	}
}
