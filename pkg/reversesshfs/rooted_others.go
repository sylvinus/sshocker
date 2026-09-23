//go:build !linux && !darwin

package reversesshfs

import (
	"errors"
	"io"

	"github.com/pkg/sftp"
)

type rootedHandlers struct{}

func newRootedServer(io.ReadWriteCloser, string, bool, []string) (*sftp.RequestServer, *rootedHandlers, error) {
	return nil, nil, errors.New("the rooted builtin sftp server is supported only on Linux and macOS")
}

func (*rootedHandlers) Close() error { return nil }

func (*rootedHandlers) expectRemove(string) {}
