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

func New(dsn string, cfg *dsync.Config) (dsync.DataSource, error) {
	var err error
	var sb strings.Builder

	ds := &pgDataSource{
		tablename:  cfg.TableName,
		basepath:   cfg.Basepath,
		setFS: cfg.FileSystem,
		successful: false,
	}

	if err = dsync.ValidateConfig(cfg); err != nil {
		return nil, err
	}

	ds.db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := ds.db.Ping(); err != nil {
		return nil, err
	}

	sb.WriteString(`CREATE TABLE "`)
	sb.WriteString(cfg.TableName)
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
	sb.WriteString(cfg.TableName)
	sb.WriteString(`"`)
	ds.selectionQuery = sb.String()
	sb.Reset()

	sb.WriteString(`INSERT INTO "`)
	sb.WriteString(cfg.TableName)
	sb.WriteString(`"`)
	sb.WriteString(`(Name, File, Version, CreatedAt, Checksum) VALUES ($1, $2, $3, $4, $5)`)
	ds.insertionQuery = sb.String()

	return ds, nil
}

func (p *pgDataSource) BeginTransaction() error {
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

func (p *pgDataSource) SetTransactionSuccessful(b bool) {
	p.successful = b
}

func (p pgDataSource) EndTransaction() {
	if p.successful {
		p.tx.Commit()
	} else {
		p.tx.Rollback()
	}
}

func (p pgDataSource) GetChangeSetFileSystem() (fs.FS, error) {
	return p.setFS, nil
}

func (p pgDataSource) GetMigrationInfo() (*dsync.MigrationInfo, error) {
	// Connect
	q := `select exists(select 1
		from information_schema."tables"
		where is_insertable_into = 'YES' 
		and table_type = 'BASE TABLE' 
		and table_catalog = CURRENT_CATALOG 
		and table_name = $1 
	)	
	`
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
		return &dsync.MigrationInfo{TableName: p.tablename, Migrations: migrations}, nil
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

func (p pgDataSource) ApplyMigration(m *dsync.Migration) error {
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
					return err
				}
				m.Success = true
				return p.logMigration(m)
			} else {
				return err
			}
		} else {
			sb.Write(buf[:l])
		}
	}
}

func (p pgDataSource) GetPath() string {
	return p.basepath
}

func (p pgDataSource) logMigration(m *dsync.Migration) error {
	_, err := p.tx.Exec(p.insertionQuery, m.Name, m.File, m.Version, m.CreatedAt, m.Checksum)
	return err
}
