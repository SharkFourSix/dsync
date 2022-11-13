package dsync_test

import (
	"embed"
	"testing"

	"github.com/SharkFourSix/dsync"
	"github.com/SharkFourSix/dsync/sources/postgresql"
)

//go:embed resources/migrations
var e embed.FS

const BASEPATH string = "resources/migrations"

func TestPostgresqlDataSource(t *testing.T) {
	dsn := "postgres://postgres:toor@localhost:5433/test-db"
	migrator := dsync.Migrator{OutOfOrder: true}
	
	ds, err := postgresql.New(dsn, &dsync.Config{
		FileSystem: e,
		Basepath:   BASEPATH,
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
