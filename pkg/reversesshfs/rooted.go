//go:build linux || darwin

package reversesshfs

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/sys/unix"
)

// rootedHandlers serves only the files under rootPath.
//
// Reads go through os.Root, which follows symlinks but never outside the root.
// Writes open the parent directory one component at a time with O_NOFOLLOW,
// so they never follow a symlink, and are denied when any path component
// matches readonlyNames. Following no symlink on writes is what prevents
// the client from swapping a directory for a symlink into a read-only one.
type rootedHandlers struct {
	rootPath      string // slash-separated, cleaned
	root          *os.Root
	rootFD        int
	readonly      bool
	readonlyNames []string

	mu           sync.Mutex
	noopRemovals map[string]time.Time // expiry, keyed by request path
}

// noopRemovalTTL bounds how long an ExpectRemove token waits for the guest.
const noopRemovalTTL = 5 * time.Second

func newRootedServer(rwc io.ReadWriteCloser, localPath string, readonly bool, readonlyNames []string) (*sftp.RequestServer, *rootedHandlers, error) {
	root, err := os.OpenRoot(localPath)
	if err != nil {
		return nil, nil, err
	}
	rootFD, err := unix.Open(localPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		root.Close()
		return nil, nil, &os.PathError{Op: "open", Path: localPath, Err: err}
	}
	h := &rootedHandlers{
		rootPath:      path.Clean(filepath.ToSlash(localPath)),
		root:          root,
		rootFD:        rootFD,
		readonly:      readonly,
		readonlyNames: readonlyNames,
		noopRemovals:  make(map[string]time.Time),
	}
	handlers := sftp.Handlers{FileGet: h, FilePut: h, FileCmd: h, FileList: h}
	srv := sftp.NewRequestServer(rwc, handlers, sftp.WithStartDirectory(h.rootPath))
	return srv, h, nil
}

func (h *rootedHandlers) Close() error {
	return errors.Join(h.root.Close(), unix.Close(h.rootFD))
}

// expectRemove makes the next Remove or Rmdir request for p, within noopRemovalTTL,
// succeed without touching the host. p is a host path under the root.
func (h *rootedHandlers) expectRemove(p string) {
	p = path.Clean(filepath.ToSlash(p))
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, expiry := range h.noopRemovals {
		if now.After(expiry) {
			delete(h.noopRemovals, k)
		}
	}
	h.noopRemovals[p] = now.Add(noopRemovalTTL)
}

func (h *rootedHandlers) consumeNoopRemoval(p string) bool {
	p = path.Clean(p)
	h.mu.Lock()
	defer h.mu.Unlock()
	expiry, ok := h.noopRemovals[p]
	if !ok {
		return false
	}
	delete(h.noopRemovals, p)
	return time.Now().Before(expiry)
}

// rel maps a request path to a path relative to the root.
// The result has no "." or ".." component, except "." for the root itself.
func (h *rootedHandlers) rel(p string) (string, error) {
	if !path.IsAbs(p) {
		p = path.Join(h.rootPath, p)
	}
	p = path.Clean(p)
	if p == h.rootPath {
		return ".", nil
	}
	prefix := h.rootPath
	if prefix != "/" {
		prefix += "/"
	}
	if r, ok := strings.CutPrefix(p, prefix); ok {
		return r, nil
	}
	return "", unix.EACCES
}

func (h *rootedHandlers) writableRel(p string) (string, error) {
	if h.readonly {
		return "", unix.EACCES
	}
	r, err := h.rel(p)
	if err != nil {
		return "", err
	}
	if h.isReadonlyName(r) {
		return "", unix.EACCES
	}
	return r, nil
}

func (h *rootedHandlers) isReadonlyName(rel string) bool {
	for _, c := range strings.Split(rel, "/") {
		for _, name := range h.readonlyNames {
			if sameName(c, name) {
				return true
			}
		}
	}
	return false
}

// sameName reports whether a file name may refer to the same entry as name
// on a case-insensitive (APFS, HFS+) file system.
// The ignored code points are the ones listed in next_hfs_char() of git's utf8.c.
func sameName(s, name string) bool {
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 0x200c && r <= 0x200f, r >= 0x202a && r <= 0x202e, r >= 0x206a && r <= 0x206f, r == 0xfeff:
			return -1
		}
		return r
	}, s)
	return strings.EqualFold(s, name)
}

// openParent returns a directory fd for the parent of rel, and the base name.
// The caller must close the fd.
func (h *rootedHandlers) openParent(rel string) (int, string, error) {
	fd, err := unix.Dup(h.rootFD)
	if err != nil {
		return -1, "", err
	}
	if rel == "." {
		return fd, ".", nil
	}
	dir, base := path.Split(rel)
	if dir != "" {
		for _, c := range strings.Split(strings.TrimSuffix(dir, "/"), "/") {
			if c == "" || c == "." || c == ".." {
				unix.Close(fd)
				return -1, "", unix.EACCES
			}
			next, err := unix.Openat(fd, c, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
			unix.Close(fd)
			if err != nil {
				return -1, "", err
			}
			fd = next
		}
	}
	return fd, base, nil
}

// Fileread implements sftp.FileReader.
func (h *rootedHandlers) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	rel, err := h.rel(r.Filepath)
	if err != nil {
		return nil, err
	}
	return h.root.Open(rel)
}

// Filewrite implements sftp.FileWriter.
func (h *rootedHandlers) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	return h.openFile(r)
}

// OpenFile implements sftp.OpenFileWriter.
func (h *rootedHandlers) OpenFile(r *sftp.Request) (sftp.WriterAtReaderAt, error) {
	return h.openFile(r)
}

func (h *rootedHandlers) openFile(r *sftp.Request) (*os.File, error) {
	rel, err := h.writableRel(r.Filepath)
	if err != nil {
		return nil, err
	}
	pf := r.Pflags()
	flags := unix.O_NOFOLLOW | unix.O_CLOEXEC
	switch {
	case pf.Read && (pf.Write || pf.Append):
		flags |= unix.O_RDWR
	case pf.Write || pf.Append:
		flags |= unix.O_WRONLY
	default:
		flags |= unix.O_RDONLY
	}
	// O_APPEND is not set, as it conflicts with WriteAt; the client sends offsets.
	if pf.Creat {
		flags |= unix.O_CREAT
	}
	if pf.Trunc {
		flags |= unix.O_TRUNC
	}
	if pf.Excl {
		flags |= unix.O_EXCL
	}
	var mode uint32 = 0o644
	if r.AttrFlags().Permissions {
		mode = r.Attributes().Mode & 0o7777
	}
	dirfd, base, err := h.openParent(rel)
	if err != nil {
		return nil, err
	}
	defer unix.Close(dirfd)
	fd, err := unix.Openat(dirfd, base, flags, mode)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: r.Filepath, Err: err}
	}
	return os.NewFile(uintptr(fd), r.Filepath), nil
}

// Filecmd implements sftp.FileCmder.
func (h *rootedHandlers) Filecmd(r *sftp.Request) error {
	switch r.Method {
	case "Setstat":
		return h.setstat(r)
	case "Rename":
		return h.rename(r, true)
	case "Link":
		return h.twoPaths(r.Filepath, r.Target, func(oldfd int, oldBase string, newfd int, newBase string) error {
			return unix.Linkat(oldfd, oldBase, newfd, newBase, 0)
		})
	case "Remove", "Rmdir":
		// The guest agent removes a path deleted on the host, so that the guest emits IN_DELETE.
		// The path may have been created again on the host since, so it must not be removed.
		if h.consumeNoopRemoval(r.Filepath) {
			return nil
		}
	}
	// Symlink has the link target in Filepath and the link path in Target.
	p := r.Filepath
	if r.Method == "Symlink" {
		p = r.Target
	}
	rel, err := h.writableRel(p)
	if err != nil {
		return err
	}
	dirfd, base, err := h.openParent(rel)
	if err != nil {
		return err
	}
	defer unix.Close(dirfd)
	switch r.Method {
	case "Rmdir":
		err = unix.Unlinkat(dirfd, base, unix.AT_REMOVEDIR)
	case "Remove":
		err = unix.Unlinkat(dirfd, base, 0)
	case "Mkdir":
		var mode uint32 = 0o755
		if r.AttrFlags().Permissions {
			mode = r.Attributes().Mode & 0o7777
		}
		err = unix.Mkdirat(dirfd, base, mode)
	case "Symlink":
		err = unix.Symlinkat(r.Filepath, dirfd, base)
	default:
		return sftp.ErrSSHFxOpUnsupported
	}
	if err != nil {
		return &os.PathError{Op: strings.ToLower(r.Method), Path: p, Err: err}
	}
	return nil
}

// PosixRename implements sftp.PosixRenameFileCmder.
func (h *rootedHandlers) PosixRename(r *sftp.Request) error {
	return h.rename(r, false)
}

func (h *rootedHandlers) rename(r *sftp.Request, noReplace bool) error {
	return h.twoPaths(r.Filepath, r.Target, func(oldfd int, oldBase string, newfd int, newBase string) error {
		if noReplace {
			var st unix.Stat_t
			if err := unix.Fstatat(newfd, newBase, &st, unix.AT_SYMLINK_NOFOLLOW); err == nil {
				return os.ErrExist
			}
		}
		return unix.Renameat(oldfd, oldBase, newfd, newBase)
	})
}

func (h *rootedHandlers) twoPaths(oldPath, newPath string, f func(oldfd int, oldBase string, newfd int, newBase string) error) error {
	oldRel, err := h.writableRel(oldPath)
	if err != nil {
		return err
	}
	newRel, err := h.writableRel(newPath)
	if err != nil {
		return err
	}
	oldfd, oldBase, err := h.openParent(oldRel)
	if err != nil {
		return err
	}
	defer unix.Close(oldfd)
	newfd, newBase, err := h.openParent(newRel)
	if err != nil {
		return err
	}
	defer unix.Close(newfd)
	if err := f(oldfd, oldBase, newfd, newBase); err != nil {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: err}
	}
	return nil
}

func (h *rootedHandlers) setstat(r *sftp.Request) error {
	if h.isNoopTimes(r) {
		return nil
	}
	rel, err := h.writableRel(r.Filepath)
	if err != nil {
		return err
	}
	dirfd, base, err := h.openParent(rel)
	if err != nil {
		return err
	}
	defer unix.Close(dirfd)
	flags := r.AttrFlags()
	attrs := r.Attributes()
	if flags.Size {
		fd, err := unix.Openat(dirfd, base, unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
		if err != nil {
			return &os.PathError{Op: "truncate", Path: r.Filepath, Err: err}
		}
		err = unix.Ftruncate(fd, int64(attrs.Size))
		unix.Close(fd)
		if err != nil {
			return &os.PathError{Op: "truncate", Path: r.Filepath, Err: err}
		}
	}
	if flags.Permissions {
		if err := fchmodatNoFollow(dirfd, base, attrs.Mode&0o7777); err != nil {
			return &os.PathError{Op: "chmod", Path: r.Filepath, Err: err}
		}
	}
	if flags.UidGid {
		if err := unix.Fchownat(dirfd, base, int(attrs.UID), int(attrs.GID), unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return &os.PathError{Op: "chown", Path: r.Filepath, Err: err}
		}
	}
	if flags.Acmodtime {
		ts := []unix.Timespec{
			unix.NsecToTimespec(int64(attrs.Atime) * 1e9),
			unix.NsecToTimespec(int64(attrs.Mtime) * 1e9),
		}
		if err := unix.UtimesNanoAt(dirfd, base, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return &os.PathError{Op: "chtimes", Path: r.Filepath, Err: err}
		}
	}
	return nil
}

// isNoopTimes reports whether r only sets the access and modification times of a
// read-only name to its current modification time. Such a request is answered
// without touching the file, so that the guest kernel still emits IN_ATTRIB:
// this is how the guest agent relays host inotify events (mountInotify).
func (h *rootedHandlers) isNoopTimes(r *sftp.Request) bool {
	flags := r.AttrFlags()
	if h.readonly || flags.Size || flags.UidGid || flags.Permissions || !flags.Acmodtime {
		return false
	}
	rel, err := h.rel(r.Filepath)
	if err != nil || !h.isReadonlyName(rel) {
		return false
	}
	fi, err := h.root.Lstat(rel)
	if err != nil {
		return false
	}
	// SFTP v3 times are in seconds.
	mtime := uint32(fi.ModTime().Unix())
	attrs := r.Attributes()
	return attrs.Atime == mtime && attrs.Mtime == mtime
}

// StatVFS implements sftp.StatVFSFileCmder.
func (h *rootedHandlers) StatVFS(r *sftp.Request) (*sftp.StatVFS, error) {
	if _, err := h.rel(r.Filepath); err != nil {
		return nil, err
	}
	var st unix.Statfs_t
	if err := unix.Fstatfs(h.rootFD, &st); err != nil {
		return nil, err
	}
	return statVFS(&st), nil
}

// Filelist implements sftp.FileLister.
func (h *rootedHandlers) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	rel, err := h.rel(r.Filepath)
	if err != nil {
		return nil, err
	}
	switch r.Method {
	case "List":
		f, err := h.root.Open(rel)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		fis, err := f.Readdir(-1)
		if err != nil {
			return nil, err
		}
		return listerAt(fis), nil
	case "Stat":
		fi, err := h.root.Stat(rel)
		if err != nil {
			return nil, err
		}
		return listerAt{fi}, nil
	}
	return nil, sftp.ErrSSHFxOpUnsupported
}

// Lstat implements sftp.LstatFileLister.
func (h *rootedHandlers) Lstat(r *sftp.Request) (sftp.ListerAt, error) {
	rel, err := h.rel(r.Filepath)
	if err != nil {
		return nil, err
	}
	fi, err := h.root.Lstat(rel)
	if err != nil {
		return nil, err
	}
	return listerAt{fi}, nil
}

// Readlink implements sftp.ReadlinkFileLister.
func (h *rootedHandlers) Readlink(p string) (string, error) {
	rel, err := h.rel(p)
	if err != nil {
		return "", err
	}
	return h.root.Readlink(rel)
}

// RealPath implements sftp.RealPathFileLister.
// It does not resolve symlinks, and does not access the file system.
func (h *rootedHandlers) RealPath(p string) (string, error) {
	if !path.IsAbs(p) {
		p = path.Join(h.rootPath, p)
	}
	return path.Clean(p), nil
}

type listerAt []os.FileInfo

func (l listerAt) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(ls, l[offset:])
	if n < len(ls) {
		return n, io.EOF
	}
	return n, nil
}
