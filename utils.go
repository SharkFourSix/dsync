package dsync

import (
	"hash/crc32"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

type state int

const (
	stateReadVersion state = iota
	stateReadName
	stateReadSeparators
)

type parserError struct {
	pos      int
	filename string
}

func (pe parserError) Error() string {
	return pe.filename + ": invalid character in migration file name at " + strconv.FormatInt(int64(pe.pos), 10)
}

// ParseMigration Parse migration information from file name
func ParseMigration(filename string) (*Migration, error) {

	var pos = 0
	var migration Migration
	var separatorsCount = 0
	var builder strings.Builder
	var _state = stateReadVersion

	reader := strings.NewReader(filename)

	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				switch _state {
				case stateReadName:
					migration.File = filename
					migration.Name = builder.String()
					return &migration, nil
				case stateReadSeparators:
					fallthrough
				case stateReadVersion:
					return nil, parserError{pos: pos, filename: filename}
				}
			} else {
				return nil, errors.Wrap(err, "error parsing migration file info")
			}
		}
		switch _state {
		case stateReadVersion:
			if !unicode.IsDigit(r) {
				_version, err := strconv.ParseInt(builder.String(), 10, 64)
				if err != nil {
					return nil, errors.Wrapf(err, "%s:%d error parsing migration file name", filename, pos)
				}
				_state = stateReadSeparators
				reader.UnreadRune()
				migration.Version = _version
				builder.Reset()
			} else {
				builder.WriteRune(r)
			}
		case stateReadSeparators:
			if r != '_' {
				if separatorsCount == 2 {
					_state = stateReadName
					reader.UnreadRune()
				} else {
					return nil, parserError{pos: pos, filename: filename}
				}
			} else if separatorsCount > 2 {
				return nil, parserError{pos: pos, filename: filename}
			} else {
				separatorsCount++
			}
		case stateReadName:
			builder.WriteRune(r)
		}
		pos++
	}
}

// HashFile Calculate file content checksum using CRC32(IEEE)
func HashFile(_fs fs.FS, filename string) (int64, error) {
	var buf []byte
	var h = crc32.New(crc32.MakeTable(crc32.IEEE))

	buf = make([]byte, 1024)

	file, err := _fs.Open(filename)
	if err != nil {
		return 0, errors.Wrap(err, "failed to calculate file hash")
	}
	defer file.Close()

	for {
		r, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				return int64(h.Sum32()), nil
			}
			return 0, err
		}
		h.Write(buf[:r])
	}
}

// ExtractVersion Extract version from a migration changeset file
func ExtractVersion(filename string) (version int64, err error) {
	versionString, _, found := strings.Cut(filename, "__")
	if !found {
		return 0, errors.Errorf("invalid file format")
	}
	versionString = strings.TrimLeft(versionString, "0")
	version, err = strconv.ParseInt(versionString, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "error parsing version number %q", filename)
	}
	return
}

// SortDirectoryEntries Sorts the slice in place using the library's naming scheme.
//
// NOTE: This function should not be treated as a validation function.
func SortDirectoryEntries(entries []fs.DirEntry) (status error) {
	defer func() {
		p := recover()
		if p != nil {
			status = p.(error)
		}
	}()
	slices.SortStableFunc(entries, func(a, b fs.DirEntry) int {
		entryAVersion, err := ExtractVersion(a.Name())
		if err != nil {
			panic(err)
		}
		entryBVersion, err := ExtractVersion(b.Name())
		if err != nil {
			panic(err)
		}
		return int(entryAVersion - entryBVersion)
	})
	return
}
