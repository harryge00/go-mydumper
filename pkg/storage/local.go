package storage

import (
	"fmt"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"io"
	"io/ioutil"
	"os"
)

// LocalStorage represents local file system storage.
//
// export for using in tests.
type LocalStorage struct {
	base string
}

// Write file to local file system.
func (l *LocalStorage) WriteFile(name string, data string) error {
	filename := fmt.Sprintf("%s/%s", l.base, name)
	flag := os.O_RDWR | os.O_TRUNC
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(filename, flag, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(common.StringToBytes(data))
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (l *LocalStorage) ReadFile(name string) ([]byte, error) {
	filename := fmt.Sprintf("%s/%s", l.base, name)
	return ioutil.ReadFile(filename)
}

func NewLocalStorage(base string) (*LocalStorage, error) {
	// Create directory if not exist
	if _, err := os.Stat(base); os.IsNotExist(err) {
		err := os.MkdirAll(base, 0777)
		if err != nil {
			return nil, err
		}
	}
	return &LocalStorage{base: base}, nil
}
