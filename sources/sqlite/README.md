# Sqlite Data source


### Usage

```shell
go get github.com/SharkFourSix/dsync/sources/sqlite
```

```golang
import "github.com/SharkFourSix/dsync/sources/sqlite"

dsn := "file:test.db?cache=shared&mode=rw"

pgds, err := postgresql.New(dsn, &dsync.Config{ ... })
if err != nill {
    panic(err)
}

migrator.Migrate(pgds)
```

### SQL Driver

https://github.com/mattn/go-sqlite3