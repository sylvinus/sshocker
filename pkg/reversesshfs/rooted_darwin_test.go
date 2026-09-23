package reversesshfs

import (
	"os"
	"syscall"
)

func ctime(fi os.FileInfo) syscall.Timespec {
	return fi.Sys().(*syscall.Stat_t).Ctimespec
}
