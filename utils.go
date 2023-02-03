package dsync

import (
	"hash/crc32"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

type state int

const (
	state_read_version state = iota
	state_read_name
	state_read_separators
)

type parser_error struct {
	pos      int
	filename string
}

func (pe parser_error) Error() string {
	return pe.filename + ": invalid character in migration file name at " + strconv.FormatInt(int64(pe.pos), 10)
}

// ParseMigration Parse migration information from file name
func ParseMigration(filename string) (*Migration, error) {

	var pos = 0
	var migration Migration
	var separators_count = 0
	var builder strings.Builder
	var _state state = state_read_version

	reader := strings.NewReader(filename)

	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				switch _state {
				case state_read_name:
					migration.File = filename
					migration.Name = builder.String()
					return &migration, nil
				case state_read_separators:
					fallthrough
				case state_read_version:
					return nil, parser_error{pos: pos, filename: filename}
				}
			} else {
				return nil, errors.Wrap(err, "error parsing migration file info")
			}
		}
		switch _state {
		case state_read_version:
			if !unicode.IsDigit(r) {
				_version, err := strconv.ParseInt(builder.String(), 10, 64)
				if err != nil {
					return nil, errors.Wrapf(err, "%s:%d error parsing migration file name", filename, pos)
				}
				_state = state_read_separators
				reader.UnreadRune()
				migration.Version = _version
				builder.Reset()
			} else {
				builder.WriteRune(r)
			}
		case state_read_separators:
			if r != '_' {
				if separators_count == 2 {
					_state = state_read_name
					reader.UnreadRune()
				} else {
					return nil, parser_error{pos: pos, filename: filename}
				}
			} else if separators_count > 2 {
				return nil, parser_error{pos: pos, filename: filename}
			} else {
				separators_count++
			}
		case state_read_name:
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
