package storage

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"io"
	"io/ioutil"
	"os"
)

// LocalStorage represents local file system storage.
//
// export for using in tests.
type LocalStorage struct {
}

// Write file to local file system.
func (l *LocalStorage) WriteFile(name string, data string) error {
	flag := os.O_RDWR | os.O_TRUNC
	if _, err := os.Stat(name); os.IsNotExist(err) {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(name, flag, 0644)
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
	return ioutil.ReadFile(name)
}

func NewLocalStorage() *LocalStorage {
	return &LocalStorage{}
}
