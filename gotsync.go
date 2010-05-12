// gotsync: Go Tree Sync, a massively parallel filesystem tree syncer
//
// Author: Brad Fitzpatrick <brad@danga.com>
//
// Assumes filesystem stats are slow (such as syncing a large tree
// from local disk to a remote NFS server, where dentries need to be
// re-validated remotely...) and tries to keep all possible operations
// in flight as possible, trying to get performance gains through
// parallelism.
//
// Copyright 2010 Brad Fitzpatrick. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gotsync

import "exp/iterable"
import "fmt"
import "os"
import "io"
import "syscall"

type Syncer struct {
	Verbose       bool
	ErrorWriter   io.Writer
	VerboseWriter io.Writer
}

// Constructs a new, default Syncer which writes errors to stderr
// and is not verbose.
//
// Mutate the returned environment to taste.
func New() *Syncer {
	return &Syncer{false, os.Stderr, os.Stdout}
}

type SyncStats struct {
	ErrorCount int

	FilesGood    int // no change needed
	DirsGood     int // already existed (contents might be wrong)
	SymlinksGood int

	FilesCreated    int
	DirsCreated     int
	SymlinksCreated int

	// existed, but wrong. also counts in Deleted
	FilesWrong    int
	SymlinksWrong int

	DirsDeleted  int
	FilesDeleted int
}

func (stats *SyncStats) incrementBy(delta *SyncStats) {
	stats.ErrorCount += delta.ErrorCount

	stats.FilesGood += delta.FilesGood
	stats.DirsGood += delta.DirsGood
	stats.SymlinksGood += delta.SymlinksGood

	stats.FilesCreated += delta.FilesCreated
	stats.DirsCreated += delta.DirsCreated
	stats.SymlinksCreated += delta.SymlinksCreated

	stats.FilesWrong += delta.FilesWrong
	stats.SymlinksWrong += delta.SymlinksWrong

	stats.DirsDeleted += delta.DirsDeleted
	stats.FilesDeleted += delta.FilesDeleted
}

func (stats SyncStats) String() string {
	return fmt.Sprintf(`**** Sync stats:
Errors: %d
Good/Wrong
 - files: %d/%d
 - dirs: %d
 - symlinks: %d/%d
Created:
 - files: %d
 - dirs: %d
 - symlinks: %d
Deleted:
 - files: %d
 - dirs: %d
`,
		stats.ErrorCount,
		stats.FilesGood, stats.FilesWrong,
		stats.DirsGood,
		stats.SymlinksGood, stats.SymlinksWrong,
		stats.FilesCreated,
		stats.DirsCreated,
		stats.SymlinksCreated,
		stats.FilesDeleted,
		stats.DirsDeleted)

}

type outstandingOps struct {
	ch chan SyncStats
	count int
}

func (ops *outstandingOps) new() chan SyncStats {
	ops.count++
	if ops.ch == nil {
		ops.ch = make(chan SyncStats)
	}
	return ops.ch
}

// Wait for all outstandin operations, summing the total into outStats
func (ops *outstandingOps) wait(outStats *SyncStats) {
	for i := 0; i < ops.count; i++ {
		subStats := <-ops.ch
		outStats.incrementBy(&subStats)
	}
}

func (self *Syncer) readDirnames(dir string, outNames *[]string, ok chan bool) {
	fd, err := os.Open(dir, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter, "Error reading dirnames: %v\n", err)
		ok <- false
	}
	defer fd.Close()
	names, err := fd.Readdirnames(-1)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter, "Error reading dirnames: %v\n", err)
		ok <- false
	} else {
		*outNames = names
		ok <- true
	}
}

func makeListContainsFunc(items []string) func(entry interface{}) bool {
	var set = map[string]bool{}
	for _, name := range items {
		set[name] = true
	}

	return func(entry interface{}) bool {
		name := entry.(string)
		if set[name] {
			return true
		}
		return false
	}
}

func (self *Syncer) copyRegularFile(srcName string, stat *os.FileInfo, dstName string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() { out <- *stats }()

	outfd, err := os.Open(dstName, os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		stat.Permission())
	if err != nil {
		fmt.Fprintf(self.ErrorWriter,
			"Error opening copy output file %s: %s\n",
			dstName, err)
		stats.ErrorCount++
		return
	}
	defer outfd.Close()

	infd, err := os.Open(srcName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter,
			"Error opening copy source file %s: %s\n",
			srcName, err)
		stats.ErrorCount++
		return
	}
	defer infd.Close()

	const BUF_SIZE = 1024 * 256
	buf := make([]byte, BUF_SIZE)
	bytesRemain := stat.Size
	for bytesRemain > 0 {
		n, err := infd.Read(buf)
		switch {
		case n == 0:
			break
		case n < 0:
			stats.ErrorCount++
			fmt.Fprintf(self.ErrorWriter, "Error copying file %s in read: %s",
				srcName, err)
			return
		default:
			outN, err := outfd.Write(buf[0:n])
			if err != nil || outN != n {
				fmt.Fprintf(self.ErrorWriter, "Error copying file %s in write: %s",
					srcName, err)
				return
			}
			bytesRemain -= int64(outN)
		}
	}

	// Close it explicitly before we syscall.Utime() it, even
	// though the precautionary defer'd Close() above will close
	// it again later.  That's harmless.
	err = outfd.Close()
	if err != nil {
		stats.ErrorCount++
		return
	}

	// TODO: how to do this in a portable way?  Timeval.Sec and
	// Usec are int64 on amd64.  Currently this only compiles in
	// 386.
	os.Chtimes(dstName, stat.Atime_ns, stat.Mtime_ns)
	var tv []syscall.Timeval = make([]syscall.Timeval, 2)
	tv[0] = syscall.NsecToTimeval(stat.Atime_ns)
	tv[1] = syscall.NsecToTimeval(stat.Mtime_ns)
	errno := syscall.Utimes(dstName, tv)
	if errno != 0 {
		fmt.Fprintf(self.ErrorWriter, "Error modifying utimes on %s: %v",
			dstName, errno)
		stats.ErrorCount++
		return
	}

	stats.FilesCreated++
	if self.Verbose {
		fmt.Fprintln(self.VerboseWriter, dstName)
	}
}

func (self *Syncer) copyDirectory(srcName string, stat *os.FileInfo, dstName string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() { out <- *stats }()

	err := os.Mkdir(dstName, stat.Permission())
	if err != nil {
		stats.ErrorCount++
		return
	}
	stats.DirsCreated++
	if self.Verbose {
		fmt.Fprintln(self.VerboseWriter, dstName)
	}

	ops := new(outstandingOps)
	go self.SyncDirectories(srcName, dstName, ops.new())
	ops.wait(stats)
}

func sendError(out chan SyncStats) {
	stats := new(SyncStats)
	stats.ErrorCount++
	out <- *stats
}

func lstatAsync(filename string) chan *os.FileInfo {
	ch := make(chan *os.FileInfo)
	go func() {
		stat, err := os.Lstat(filename)
		if err != nil {
			ch <- nil
		} else {
			ch <- stat
		}
	}()
	return ch
}

// Stats both srcName and dstName.  If they're files and have same
// FileInfo, no changes.  If they're directories, syncs them.  Will
// destroy dstName if it needs to be replaced with the right type.
//
// To be run in a goroutine.  Writes its aggregate stats to out.
//
// Precondition:  dstName exists.
func (self *Syncer) checkOrMakeEqual(srcName string, dstName string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() { out <- *stats }()

	// Kick off async stats
	srcStatChan := lstatAsync(srcName)
	dstStatChan := lstatAsync(dstName)

	srcStat := <-srcStatChan
	dstStat := <-dstStatChan
	if srcStat == nil || dstStat == nil {
		stats.ErrorCount++
		return
	}

	if srcStat.IsRegular() {
		if dstStat.IsRegular() {
			// TODO: check uid/gid too probably
			if srcStat.Size == dstStat.Size &&
				srcStat.Mtime_ns == dstStat.Mtime_ns &&
				srcStat.Mode == dstStat.Mode {
				stats.FilesGood++
				return
			}
		}
		stats.FilesWrong++
	} else if srcStat.IsDirectory() {
		if dstStat.IsDirectory() {
			stats.DirsGood++
			ops := new(outstandingOps)
			go self.SyncDirectories(srcName, dstName, ops.new())
			ops.wait(stats)
			return
		}
	} else if srcStat.IsSymlink() {
		if dstStat.IsSymlink() {
			// TODO: do readlinks async
			srcTarget, errSrc := os.Readlink(srcName)
			dstTarget, errDst := os.Readlink(dstName)
			if errSrc == nil && errDst == nil && srcTarget == dstTarget {
				stats.SymlinksGood++
				return
			}
		}
		stats.SymlinksWrong++
	} else {
		fmt.Fprintf(self.ErrorWriter, "Unhandled filetype %s\n", srcName)
		stats.ErrorCount++
		return
	}

	// Kill whatever it was...
	ops := new(outstandingOps)
	go self.RemoveAll(dstName, ops.new())
	ops.wait(stats)

	// And copy it over..
	ops = new(outstandingOps)
	go self.Copy(srcName, dstName, ops.new())
	ops.wait(stats)
}

// Copy srcName to dstName, whatever srcName happens to be.
//
// Writes its aggregate stats to out.
//
// Precondition:  dstName doesn't exist
func (self *Syncer) Copy(srcName string, dstName string, out chan SyncStats) {
	srcStat, serr := os.Lstat(srcName)
	if serr != nil {
		fmt.Fprintf(self.ErrorWriter, "Can't stat source %s: %v\n",
			srcName, serr)
		sendError(out)
		return
	}

	switch {
	case srcStat.IsRegular():
		self.copyRegularFile(srcName, srcStat, dstName, out)

	case srcStat.IsDirectory():
		self.copyDirectory(srcName, srcStat, dstName, out)

	case srcStat.IsSymlink():
		target, lerr := os.Readlink(srcName)
		if lerr != nil {
			sendError(out)
			return
		}
		lerr = os.Symlink(target, dstName)
		if lerr != nil {
			fmt.Fprintf(self.ErrorWriter, "Error making symlink %s: %v\n",
				dstName, lerr)
			sendError(out)
			return
		}
		stats := new(SyncStats)
		stats.SymlinksCreated++
		out <- *stats

	default:
		// TODO: symlinks, etc
		fmt.Fprintf(self.ErrorWriter, "Can't handle special file %s\n",
			srcName)
		sendError(out)
	}
}

// Deletes filename, and recursively if a directory.
//
// This is similar to os.RemoveAll, except this version runs
// concurrently on all files and subdirectories when deleting
// directories and recursing into them to delete them.
//
// Aggregate stats are written to the provided channel.
func (self *Syncer) RemoveAll(filename string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() {
		out <- *stats
	}()

	dirstat, err := os.Lstat(filename)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter, "Can't stat %s: %v", filename, err)
		stats.ErrorCount++
		return
	}

	// Leaf case: if Remove works, we're done.
	err = os.Remove(filename)
	if err == nil {
		if self.Verbose {
			fmt.Fprintln(self.VerboseWriter, "x", filename)
		}
		if dirstat.IsDirectory() {
			stats.DirsDeleted++
		} else {
			stats.FilesDeleted++
		}
		return
	}

	// Otherwise, is this a directory we need to recurse into?
	if !dirstat.IsDirectory() {
		fmt.Fprintf(self.ErrorWriter, "Not a directory as expected: %s",
			filename)
		stats.ErrorCount++
		return
	}

	fd, err := os.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter, "Error opening dir %s: %v",
			filename, err)
		stats.ErrorCount++
		return
	}

	names, err := fd.Readdirnames(-1)
	if err != nil {
		fmt.Fprintf(self.ErrorWriter, "Error readdir dir %s: %v",
			filename, err)
		fd.Close()
		stats.ErrorCount++
		return
	}
	fd.Close()

	ops := new(outstandingOps)
	for _, name := range names {
		go self.RemoveAll(fmt.Sprintf("%s/%s", filename, name), ops.new())
	}
	ops.wait(stats)

	// Delete the final directory
	err = os.Remove(filename)
	if err != nil {
		stats.ErrorCount++
	} else {
		stats.DirsDeleted++
	}
}

func removeItem(items []string, item string) []string {
	for i, ele := range items {
		if ele == item {
			items[i] = items[len(items)-1]
			return items[0:len(items)-1]
		}
	}
	return items
}

func (self *Syncer) SyncDirectories(srcDir string, dstDir string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() { out <- *stats }()

	var srcDirnames []string
	var dstDirnames []string
	srcReadOp := make(chan bool)
	dstReadOp := make(chan bool)
	go self.readDirnames(srcDir, &srcDirnames, srcReadOp)
	go self.readDirnames(dstDir, &dstDirnames, dstReadOp)
	if !<-srcReadOp {
		stats.ErrorCount++
		fmt.Fprintf(self.ErrorWriter, "Error reading %v\n", srcDir)
	}
	if !<-dstReadOp {
		stats.ErrorCount++
		fmt.Fprintf(self.ErrorWriter, "Error reading %v\n", dstDir)
	}
	if stats.ErrorCount != 0 {
		return
	}

	srcDirnames = removeItem(srcDirnames, ".repo")
	srcDirnames = removeItem(srcDirnames, ".git")

	inSourceDir := makeListContainsFunc(srcDirnames)
	inDestDir := makeListContainsFunc(dstDirnames)
	notInSourceDir := func(entry interface{}) bool {
		return !inSourceDir(entry)
	}

	srcNamesIter := iterable.StringArray(srcDirnames)
	dstNamesIter := iterable.StringArray(dstDirnames)
	inDest, notInDest := iterable.Partition(srcNamesIter, inDestDir)

	ops := new(outstandingOps)

	fileInSrc := func(s interface{}) string {
		str := s.(string)
		return fmt.Sprintf("%s/%s", srcDir, str)
	}
	fileInDest := func(s interface{}) string {
		str := s.(string)
		return fmt.Sprintf("%s/%s", dstDir, str)
	}

	// Files in destination which may already be correct...
	for e := range inDest.Iter() {
		go self.checkOrMakeEqual(fileInSrc(e), fileInDest(e), ops.new())
	}

	// Files which don't exist in destination and need to be
	// copied over..
	for e := range notInDest.Iter() {
		go self.Copy(fileInSrc(e), fileInDest(e), ops.new())
	}

	// Files in destination which shouldn't exist...
	toBeDeletedNames := iterable.Filter(dstNamesIter, notInSourceDir)
	for e := range toBeDeletedNames.Iter() {
		go self.RemoveAll(fileInDest(e), ops.new())
	}

	ops.wait(stats)
}
