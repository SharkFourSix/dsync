package sqlite

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/SharkFourSix/dsync"
	_ "github.com/mattn/go-sqlite3"
)

type sqliteDataSource struct {
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

func New(dsn string, cfg *dsync.Config) (dsync.DataSource, error) {
	var err error
	var sb strings.Builder

	ds := &sqliteDataSource{
		tablename:  cfg.TableNameOrDefault(),
		basepath:   cfg.Basepath,
		setFS:      cfg.FileSystem,
		successful: false,
	}

	if err = dsync.ValidateConfig(cfg); err != nil {
		return nil, err
	}

	ds.db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	if err := ds.db.Ping(); err != nil {
		return nil, err
	}

	sb.WriteString(`CREATE TABLE "`)
	sb.WriteString(ds.tablename)
	sb.WriteString(`"`)
	sb.WriteString(`(Id INTEGER PRIMARY KEY AUTOINCREMENT
		, Name TEXT NOT NULL
		, File TEXT NOT NULL
		, Version INTEGER NOT NULL
		, CreatedAt TIMESTAMP
		, Checksum INTEGER NOT NULL)`,
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

func (p *sqliteDataSource) BeginTransaction() error {
	if p.tx != nil {
		return errors.New("already in transaction")
	}
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	p.tx = tx
	return nil
}

func (p *sqliteDataSource) SetTransactionSuccessful(b bool) {
	p.successful = b
}

func (p sqliteDataSource) EndTransaction() {
	if p.successful {
		p.tx.Commit()
	} else {
		p.tx.Rollback()
	}
}

func (p sqliteDataSource) GetChangeSetFileSystem() (fs.FS, error) {
	return p.setFS, nil
}

func (p sqliteDataSource) GetMigrationInfo() (*dsync.MigrationInfo, error) {
	// Connect

	q := `select exists(select 1 from sqlite_master where type = 'table' and name = $1)`
	var currentVersion int64
	var exists bool
	if err := p.db.QueryRow(q, p.tablename).Scan(&exists); err != nil {
		return nil, err
	}

	if exists {
		var migrations []dsync.Migration
		r, err := p.db.Query(p.selectionQuery)
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
		return &dsync.MigrationInfo{TableName: p.tablename, Migrations: migrations, Version: currentVersion}, nil
	} else {
		_, err := p.db.Exec(p.createTableQuery)
		if err != nil {
			return nil, err
		}
		return &dsync.MigrationInfo{
			TableName: p.tablename,
		}, nil
	}
}

func (p sqliteDataSource) ApplyMigration(m *dsync.Migration) error {
	var buf []byte
	var sb strings.Builder
	f, err := p.setFS.Open(filepath.Join(p.basepath, m.File))

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
				_, err := p.tx.Exec(query)
				if err != nil {
					return &dsync.MigrationError{Err: err, Migration: m}
				}
				m.Success = true
				return p.logMigration(m)
			} else {
				return &dsync.MigrationError{Err: err, Migration: m}
			}
		} else {
			sb.Write(buf[:l])
		}
	}
}

func (p sqliteDataSource) GetPath() string {
	return p.basepath
}

func (p sqliteDataSource) logMigration(m *dsync.Migration) error {
	_, err := p.tx.Exec(p.insertionQuery, m.Name, m.File, m.Version, m.CreatedAt, m.Checksum)
	if err != nil {
		return &dsync.MigrationError{Err: err, Migration: m}
	}
	return nil
}
