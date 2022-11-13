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
	err_migration_conflict verification_error = iota
	err_migration_match
	err_migration_missing
)

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

func (cgf *Config) validate() error {
	if cgf.FileSystem == nil {
		return errors.New("missing migration changeset source")
	}

	if len(strings.TrimSpace(cgf.Basepath)) == 0 {
		return errors.New("empty basepath")
	}

	return nil
}

func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("null configuration")
	}
	return cfg.validate()
}

type Migrator struct {
}

func verifyMigration(m *Migration, migrations []Migration) (verification_error, *Migration) {
	for _, migration := range migrations {
		if strings.EqualFold(m.File, migration.File) {
			if m.Checksum == migration.Checksum {
				return err_migration_match, &migration
			}
			return err_migration_conflict, &migration
		}
	}
	return err_migration_missing, nil
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
			e, dbm := verifyMigration(m, info.Migrations)
			switch e {
			case err_migration_conflict:
				return errors.Errorf("migration file hash conflict. expected %d, found %d", dbm.Checksum, m.Checksum)
			case err_migration_match:
				// log.info("verified version %s", m.Name)
			case err_migration_missing:
				if err := ds.ApplyMigration(m); err != nil {
					return errors.Wrap(err, "migration failed")
				}
			}
		}
	}

	ds.SetTransactionSuccessful(true)

	return nil
}
