# Postgresql Data source


### Usage

```shell
go get github.com/SharkFourSix/dsync/sources/postgresql
```

```golang
import "github.com/SharkFourSix/dsync/sources/postgresql"

dsn := "postgresql://user:password@host/database"

pgds, err := postgresql.New(dsn, &dsync.Config{ ... })
if err != nill {
    panic(err)
}

migrator.Migrate(pgds)
```