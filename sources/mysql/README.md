# MySQL Data source


### Usage

```shell
go get github.com/SharkFourSix/dsync/sources/mysql
```

```golang
import "github.com/SharkFourSix/dsync/sources/mysql"

dsn := "username:password@tcp(localhost)/database?parseTime=true"

ds, err := mysql.New(dsn, &dsync.Config{ ... })
if err != nill {
    panic(err)
}

migrator.Migrate(ds)
```

### Sql Driver

https://github.com/go-sql-driver

#### DSN Format Specifications

`[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`

**NOTE**: You must pass the `?parseTime=true` option for the driver to properly parse `TIMESTAMP` to `time.Time`.

**More information**: https://github.com/go-sql-driver/mysql#dsn-data-source-name