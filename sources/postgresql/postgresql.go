package postgresql

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/SharkFourSix/dsync"
	_ "github.com/lib/pq"
)

type pgDataSource struct {
	db               *sql.DB
	tx               *sql.Tx
	basepath         string
	successful       bool
	setFS            fs.FS
	tablename        string
	createTableQuery string
	selectionQuery   string
	insertionQuery   string
}

func Wrap(db *sql.DB, cfg *dsync.Config) (dsync.DataSource, error) {
	if err := dsync.ValidateConfig(cfg); err != nil {
		return nil, err
	}
	return create(db, cfg)
}

func create(db *sql.DB, cfg *dsync.Config) (dsync.DataSource, error) {
	var sb strings.Builder

	if err := db.Ping(); err != nil {
		return nil, err
	}

	ds := &pgDataSource{
		db:         db,
		tablename:  cfg.TableNameOrDefault(),
		basepath:   cfg.Basepath,
		setFS:      cfg.FileSystem,
		successful: false,
	}

	sb.WriteString(`CREATE TABLE "`)
	sb.WriteString(ds.tablename)
	sb.WriteString(`"`)
	sb.WriteString(`(Id SERIAL PRIMARY KEY
		, Name TEXT NOT NULL
		, File TEXT NOT NULL
		, Version BIGINT NOT NULL
		, CreatedAt timestamptz
		, Checksum BIGINT NOT NULL)`,
	)
	ds.createTableQuery = sb.String()
	sb.Reset()

	sb.WriteString(`SELECT Id, Name, File, Version, CreatedAt, Checksum FROM "`)
	sb.WriteString(ds.tablename)
	sb.WriteString(`" ORDER BY Version ASC`)
	ds.selectionQuery = sb.String()
	sb.Reset()

	sb.WriteString(`INSERT INTO "`)
	sb.WriteString(ds.tablename)
	sb.WriteString(`"`)
	sb.WriteString(`(Name, File, Version, CreatedAt, Checksum) VALUES ($1, $2, $3, $4, $5)`)
	ds.insertionQuery = sb.String()

	return ds, nil
}

func New(dsn string, cfg *dsync.Config) (dsync.DataSource, error) {

	if err := dsync.ValidateConfig(cfg); err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return create(db, cfg)
}

func (ds *pgDataSource) BeginTransaction() error {
	if ds.tx != nil {
		return errors.New("already in transaction")
	}
	tx, err := ds.db.Begin()
	if err != nil {
		return err
	}
	ds.tx = tx
	return nil
}

func (ds *pgDataSource) SetTransactionSuccessful(b bool) {
	ds.successful = b
}

func (ds *pgDataSource) EndTransaction() {
	if ds.successful {
		ds.tx.Commit()
	} else {
		ds.tx.Rollback()
	}
}

func (ds *pgDataSource) GetChangeSetFileSystem() (fs.FS, error) {
	return ds.setFS, nil
}

func (ds *pgDataSource) GetMigrationInfo() (*dsync.MigrationInfo, error) {
	// Connect
	q := `select exists(select 1
		from information_schema."tables"
		where is_insertable_into = 'YES' 
		and table_type = 'BASE TABLE' 
		and table_catalog = CURRENT_CATALOG 
		and table_name = $1 
	)	
	`
	var currentVersion int64
	var exists bool
	if err := ds.db.QueryRow(q, ds.tablename).Scan(&exists); err != nil {
		return nil, err
	}

	if exists {
		var migrations []dsync.Migration
		r, err := ds.db.Query(ds.selectionQuery)
		if err != nil {
			return nil, err
		}
		for r.Next() {
			var migration dsync.Migration
			err := r.Scan(&migration.Id, &migration.Name, &migration.File, &migration.Version, &migration.CreatedAt, &migration.Checksum)
			if err != nil {
				return nil, err
			}
			migrations = append(migrations, migration)
		}
		l := len(migrations)
		if l > 0 {
			currentVersion = migrations[l-1].Version
		}
		return &dsync.MigrationInfo{TableName: ds.tablename, Migrations: migrations, Version: currentVersion}, nil
	} else {
		_, err := ds.db.Exec(ds.createTableQuery)
		if err != nil {
			return nil, err
		}
		return &dsync.MigrationInfo{
			TableName: ds.tablename,
		}, nil
	}
}

func (ds *pgDataSource) ApplyMigration(m *dsync.Migration) error {
	var buf []byte
	var sb strings.Builder
	f, err := ds.setFS.Open(filepath.Join(ds.basepath, m.File))

	m.Success = false
	m.CreatedAt = time.Now()

	if err != nil {
		return nil
	}

	defer f.Close()

	buf = make([]byte, 1024)
	for {
		l, err := f.Read(buf)
		if err != nil {
			if err == io.EOF {
				query := sb.String()
				_, err := ds.tx.Exec(query)
				if err != nil {
					return &dsync.MigrationError{Err: err, Migration: m}
				}
				m.Success = true
				return ds.logMigration(m)
			} else {
				return &dsync.MigrationError{Err: err, Migration: m}
			}
		} else {
			sb.Write(buf[:l])
		}
	}
}

func (ds *pgDataSource) GetPath() string {
	return ds.basepath
}

func (ds *pgDataSource) logMigration(m *dsync.Migration) error {
	_, err := ds.tx.Exec(ds.insertionQuery, m.Name, m.File, m.Version, m.CreatedAt, m.Checksum)
	if err != nil {
		return &dsync.MigrationError{Err: err, Migration: m}
	}
	return nil
}

func (ds *pgDataSource) Handle() *sql.DB {
	return ds.db
}
