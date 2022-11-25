package dsync

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type verification_error int

const (
	err_migration_checksum_mismatch verification_error = iota
	err_migration_valid
	err_new_migration
	err_migration_conflict
	err_migration_out_of_order
)

const DEFAULT_TABLE_NAME = "dsync_migration_info"

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

	// GetChangeSetFileSystem GetChangeSetFileSystem returns the source file system where migration changeset files are stored
	GetChangeSetFileSystem() (fs.FS, error)

	// GetPath GetPath Returns the base path within the file system where to
	GetPath() string

	// BeginTransaction BeginTransaction Start transaction
	BeginTransaction() error

	// SetTransactionSuccessful SetTransactionSuccessful notify the data source whether to commit or rollback when EndTransaction is called
	SetTransactionSuccessful(s bool)

	// ApplyMigration ApplyMigration Applies the given migration
	ApplyMigration(migration *Migration) error

	// EndTransaction EndTransaction Commit or rollback the active transaction
	EndTransaction()
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

func (cfg Config) TableNameOrDefault() string {
	if len(strings.TrimSpace(cfg.TableName)) > 0 {
		return cfg.TableName
	}
	return DEFAULT_TABLE_NAME
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

func (migrator Migrator) verifyFsMigration(m *Migration, migrations []Migration, currentVersion int64) (verification_error, *Migration) {
	for _, migration := range migrations {
		if strings.EqualFold(m.File, migration.File) {
			if m.Checksum == migration.Checksum {
				return err_migration_valid, &migration
			}
			return err_migration_checksum_mismatch, &migration
		}
	}

	if m.Version == currentVersion {
		return err_migration_conflict, nil
	}
	if m.Version < currentVersion {
		if migrator.OutOfOrder {
			return err_new_migration, nil
		} else {
			return err_migration_out_of_order, nil
		}
	}
	// m.Version > currentVersion
	return err_new_migration, nil
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
			m.Checksum, err = HashFile(cfs, filepath.Join(basepath, entry.Name()))
			if err != nil {
				return err
			}
			e, dbm := migrator.verifyFsMigration(m, info.Migrations, info.Version)
			switch e {
			case err_migration_checksum_mismatch:
				return errors.Errorf("%s: migration file checksum conflict. expected %d, found %d", m.File, dbm.Checksum, m.Checksum)
			case err_migration_valid:
				// log.info("verified version %s", m.Name)
			case err_new_migration:
				if err := ds.ApplyMigration(m); err != nil {
					return errors.Wrap(err, "migration failed")
				}
			case err_migration_conflict:
				return errors.Errorf("%s: migration version %d already applied", m.File, m.Version)
			case err_migration_out_of_order:
				return errors.Errorf("%s: version %d is behind current version %d. Enable out of order to migrate this script", m.File, m.Version, info.Version)

			}
		}
	}

	ds.SetTransactionSuccessful(true)

	return nil
}
