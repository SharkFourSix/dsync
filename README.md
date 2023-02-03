# dsync [![Go Report Card](https://goreportcard.com/badge/github.com/SharkFourSix/dsync)](https://goreportcard.com/report/github.com/SharkFourSix/dsync)

### Getting Started

```shell
go get github.com/SharkFourSix/dsync
```

### Usage

1. Choose a data source ([check in sources](/sources/)) or implement your own.
2. Create a `Migrator` and pass the data source to the migrator

```golang
import (
	"embed"
	"testing"

	"github.com/SharkFourSix/dsync"
	"github.com/SharkFourSix/dsync/sources/postgresql"
)

//go:embed resources/migrations
var efs embed.FS

func DoMigrate(){
    dsn := "postgres://postgres:toor@localhost:5433/test-db"
    
    // Create a migrator
    var migrator dsync.Migrator

    // Configure a data source
	ds, err := postgresql.New(dsn, &dsync.Config{
		FileSystem: efs,
		Basepath:   BASEPATH,
		TableName:  "dsync_schema_migration",
	})

	if err != nil {
        panic(err)
	}

    // Migrate
	err = migrator.Migrate(ds)
	if err != nil {
		panic(err)
		return
	}
}
```

### Things To Know

- [x] File names must use the following convention to be included when scanning:

  `\d+__\w+.sql`.

- [x] An error will be returned otherwise when the version part of the file name does not contain a number.
- [x] A migration script will not be included if it does not end with **.sql** extension
- [x] Migrations are only recorded in the database when successfull
- [x] Custom migration table name to allow different migrations for difference DB clients.
- [x] Supports out of order migrations

#### Database sources

| Database | Data source                                      | Status |
|----------|--------------------------------------------------|--------|
| Postgres | github.com/SharkFourSix/dsync/sources/postgresql | Done   |
| MySQL    | github.com/SharkFourSix/dsync/sources/mysql      | Done   |
| SQLite   | N/A                                              | TBD    |

### TODO

- [ ] Add logging and configuration
- [x] Add MySQL data source
- [ ] Add SQLite data source