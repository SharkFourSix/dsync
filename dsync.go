package dsync

import (
	"database/sql"
	"io/fs"
	"path/filepath"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type verificationError int

const (
	errMigrationChecksumMismatch verificationError = iota
	errMigrationValid
	errNewMigration
	errMigrationConflict
	errMigrationOutOfOrder
)

const DefaultTableName = "dsync_migration_info"

type Migration struct {
	Id        uint32
	Name      string
	File      string
	Version   int64
	CreatedAt time.Time
	Checksum  int64
	Success   bool
}

type MigrationInfo struct {
	TableName  string
	Migrations []Migration
	Version    int64
}

type MigrationError struct {
	Err       error
	Migration *Migration
}

func (e MigrationError) Error() string {
	var builder strings.Builder

	builder.WriteString(e.Migration.File)
	builder.WriteString(": ")
	builder.WriteString(e.Err.Error())
	return builder.String()
}

type DataSource interface {
	// GetMigrationInfo Returns table name and other information
	GetMigrationInfo() (*MigrationInfo, error)

	// GetChangeSetFileSystem returns the source file system where migration changeset files are stored
	GetChangeSetFileSystem() (fs.FS, error)

	// GetPath Returns the base path within the file system where to
	GetPath() string

	// BeginTransaction Start transaction
	BeginTransaction() error

	// SetTransactionSuccessful notify the data source whether to commit or rollback when EndTransaction is called
	SetTransactionSuccessful(s bool)

	// ApplyMigration Applies the given migration
	ApplyMigration(migration *Migration) error

	// EndTransaction Commit or rollback the active transaction
	EndTransaction()

	// Handle Return the underlying database handle
	Handle() *sql.DB
}

type Config struct {
	FileSystem fs.FS
	Basepath   string
	TableName  string
}

func (cfg *Config) validate() error {
	if cfg.FileSystem == nil {
		return errors.New("missing migration changeset source")
	}

	if len(strings.TrimSpace(cfg.Basepath)) == 0 {
		return errors.New("empty basepath")
	}

	return nil
}

func (cfg *Config) TableNameOrDefault() string {
	if len(strings.TrimSpace(cfg.TableName)) > 0 {
		return cfg.TableName
	}
	return DefaultTableName
}

func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("null configuration")
	}
	return cfg.validate()
}

type Migrator struct {
	OutOfOrder bool
}

func (migrator Migrator) verifyFsMigration(m *Migration, migrations []Migration, currentVersion int64) (verificationError, *Migration) {
	for _, migration := range migrations {
		if strings.EqualFold(m.File, migration.File) {
			if m.Checksum == migration.Checksum {
				return errMigrationValid, &migration
			}
			return errMigrationChecksumMismatch, &migration
		}
	}

	if m.Version == currentVersion {
		return errMigrationConflict, nil
	}
	if m.Version < currentVersion {
		if migrator.OutOfOrder {
			return errNewMigration, nil
		} else {
			return errMigrationOutOfOrder, nil
		}
	}
	// m.Version > currentVersion
	return errNewMigration, nil
}

func (migrator Migrator) Migrate(ds DataSource) error {
	var err error
	var cfs fs.FS
	var info *MigrationInfo
	var openFiles []fs.File

	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	info, err = ds.GetMigrationInfo()
	if err != nil {
		return err
	}

	if len(info.Migrations) > 0 && info.Version == 0 {
		return errors.Errorf(
			"current migration version %d does not correspond to number of migrations (%d).",
			info.Version,
			len(info.Migrations),
		)
	}

	cfs, err = ds.GetChangeSetFileSystem()
	if err != nil {
		return err
	}

	// resort
	sort.Slice(info.Migrations, func(i, j int) bool {
		return info.Migrations[i].Version < info.Migrations[j].Version
	})

	// get migration files
	basepath := ds.GetPath()
	entries, err := fs.ReadDir(cfs, basepath)

	if err != nil {
		return errors.Wrap(err, "error reading directory entries")
	}

	err = SortDirectoryEntries(entries)
	if err != nil {
		return err
	}

	if err := ds.BeginTransaction(); err != nil {
		return errors.Wrap(err, "migration failed.")
	}

	defer ds.EndTransaction()

	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.ToLower(filepath.Ext(entry.Name())) == ".sql" {
			m, err := ParseMigration(entry.Name())
			if err != nil {
				return err
			}
			m.Checksum, err = HashFile(cfs, path.Join(basepath, entry.Name()))
			if err != nil {
				return err
			}
			e, dbm := migrator.verifyFsMigration(m, info.Migrations, info.Version)
			switch e {
			case errMigrationChecksumMismatch:
				return errors.Errorf("%s: migration file checksum conflict. expected %d, found %d", m.File, dbm.Checksum, m.Checksum)
			case errMigrationValid:
				// log.info("verified version %s", m.Name)
			case errNewMigration:
				if err := ds.ApplyMigration(m); err != nil {
					return errors.Wrap(err, "migration failed")
				}
			case errMigrationConflict:
				return errors.Errorf("%s: migration version %d already applied", m.File, m.Version)
			case errMigrationOutOfOrder:
				return errors.Errorf("%s: version %d is behind current version %d. Enable out of order to migrate this script", m.File, m.Version, info.Version)

			}
		}
	}

	ds.SetTransactionSuccessful(true)

	return nil
}

// Migrate Perform a quick migration
func Migrate(ds DataSource, outOfOrder bool) error {
	m := Migrator{
		OutOfOrder: outOfOrder,
	}
	err := m.Migrate(ds)
	if err != nil {
		err = errors.Wrap(err, "database migration failed")
	}
	return err
}
