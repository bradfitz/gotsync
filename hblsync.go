// hblsync -- High Bandwidth & Latency directory sync tool
//
// Copyright 2010 Brad Fitzpatrick
// brad@danga.com
//
// Usage: hblsync <srcdir> <dstdir>
//

package main

import "container/vector"
import "exp/iterable"
import "fmt"
import "flag"
import "os"

var delete *bool = flag.Bool("delete", false, "delete mode (takes 1 argument)")

func usage() {
	os.Stderr.WriteString("Usage: hblsync <src_dir> <dst_dir>\n")
	os.Stderr.WriteString("       hblsync --delete <dir>\n")
	os.Exit(1)
}

func main() {
	flag.Parse()
	resultChan := make(chan SyncStats)

	// Delete mode
	if (*delete) {
		if (flag.NArg() != 1) { usage() }
		go RemoveAll(flag.Arg(0), resultChan)
		fmt.Println("Results:", <-resultChan)
		return
	}

	// Sync mode
	if (flag.NArg() != 2) {
		usage();
	}

	srcDir, dstDir := flag.Arg(0), flag.Arg(1)
	ensureDirectory("source", srcDir)
	ensureDirectory("destination", dstDir)

	go SyncDirectories(srcDir, dstDir, resultChan)
	fmt.Println("Results:", <-resultChan)
}

func ensureDirectory(which string, dirName string) {
	stat, e := os.Stat(dirName)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error stat'ing %s directory: %s\n",
			which, e.String())
		os.Exit(1)
	}
	if !stat.IsDirectory() {
		fmt.Fprintf(os.Stderr, "%s directory isn't a directory: %s\n",
			which, dirName)
		os.Exit(1)
	}
}

type SyncStats struct {
	ErrorCount int

	FilesGood int   // no change needed
	DirsGood int    // already existed (contents might be wrong)

	DirsCreated int
	FilesCreated int

	FilesReplaced int  // existed, but wrong. doesn't count in FilesDeleted
	DirsDeleted int
	FilesDeleted int
}

func (stats *SyncStats) incrementBy(delta *SyncStats) {
	stats.ErrorCount += delta.ErrorCount
	stats.DirsCreated += delta.DirsCreated
	stats.DirsDeleted += delta.DirsDeleted
	stats.DirsGood += delta.DirsGood
	stats.FilesCreated += delta.FilesCreated
	stats.FilesReplaced += delta.FilesReplaced
	stats.FilesDeleted += delta.FilesDeleted
	stats.FilesGood += delta.FilesGood
}

type outstandingOps struct {
	v vector.Vector;  // of chan SyncStats
}

func (ops *outstandingOps) new() chan SyncStats {
	ch := make(chan SyncStats)
	ops.v.Push(ch)
	return ch
}

// Wait for all outstandin operations, summing the total into outStats
func (ops *outstandingOps) wait(outStats *SyncStats) {
	for _, value := range ops.v {
		ch := value.(chan SyncStats)
		subStats := <-ch
		outStats.incrementBy(&subStats)
	}
}

func readDirnames(dir string, outNames *[]string, ok chan bool) {
	fd, err := os.Open(dir, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading dirnames: %v\n", err)
		ok <- false
	}
	defer fd.Close()
	names, err := fd.Readdirnames(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading dirnames: %v\n", err)
		ok <- false
	} else {
		*outNames = names
		ok <- true
	}
}

func stringSet(items []string) map[string]bool {
	var set = map[string]bool {}
	for _, name := range items {
		set[name] = true
	}
	return set
}

func makeMapLookupTest(someMap map[string]bool) func(entry interface{}) bool {
	return func(entry interface{}) bool {
                name := entry.(string)
		if someMap[name] {
			return true
		}
		return false
	}
}

func copyFile(srcName string, dstName string, out chan SyncStats) {
	fmt.Printf("Copying files not yet implemented-- %s -> %s\n",
		srcName, dstName)
	// TODO: implement
	sendError(out)
}

func copyDirectory(srcName string, stat *os.FileInfo, dstName string,
	           out chan SyncStats) {
	stats := new(SyncStats)
	defer func() {
		out <- *stats
	}()

	err := os.Mkdir(dstName, stat.Permission())
	if err != nil {
		stats.ErrorCount++
		return
	}
	stats.DirsCreated++

	ops := new(outstandingOps)
	go SyncDirectories(srcName, dstName, ops.new())
	ops.wait(stats)
}

func sendError(out chan SyncStats) {
	stats := new(SyncStats)
	stats.ErrorCount++
	out <- *stats
}

func Copy(srcName string, dstName string, out chan SyncStats) {
	srcstat, serr := os.Lstat(srcName)
	if serr != nil {
                fmt.Fprintf(os.Stderr, "Can't stat source %s: %v", srcName, serr)
		sendError(out)
		return
	}

	switch {
	case srcstat.IsRegular():
		copyFile(srcName, dstName, out)
	case srcstat.IsDirectory():
		copyDirectory(srcName, srcstat, dstName, out)
	default:
		// TODO: symlinks, etc
		fmt.Fprintf(os.Stderr, "Can't handle special file %s",
			srcName)
		sendError(out)
	}
}

func RemoveAll(filename string, out chan SyncStats) {
	stats := new(SyncStats)
	defer func() {
		out <- *stats
	}()
	defer func() {
		//fmt.Println("RemoveAll:", filename, stats)
	}()

	dirstat, err := os.Lstat(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't stat %s: %v", filename, err)
		stats.ErrorCount++
		return
	}

	// Leaf case: if Remove works, we're done.
	err = os.Remove(filename)
	if err == nil {
		if dirstat.IsDirectory() {
			stats.DirsDeleted++
		} else {
			stats.FilesDeleted++
		}
		return
	}

	// Otherwise, is this a directory we need to recurse into?
	if !dirstat.IsDirectory() {
		fmt.Fprintf(os.Stderr, "Not a directory as expected: %s",
			filename)
		stats.ErrorCount++
                return
	}

	fd, err := os.Open(filename, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening dir %s: %v",
			filename, err)
		stats.ErrorCount++
                return
	}

	names, err := fd.Readdirnames(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error readdir dir %s: %v",
			filename, err)
		fd.Close()
		stats.ErrorCount++
                return
	}
	fd.Close()

	ops := new(outstandingOps)
	for _, name := range names {
		go RemoveAll(fmt.Sprintf("%s/%s", filename, name), ops.new())
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

func SyncDirectories(srcDir string, dstDir string, out chan SyncStats) {
	stats := new(SyncStats)

	var srcDirnames []string
	var dstDirnames []string
	srcReadOp := make(chan bool)
	dstReadOp := make(chan bool)
	go readDirnames(srcDir, &srcDirnames, srcReadOp)
	go readDirnames(dstDir, &dstDirnames, dstReadOp)
	if !<-srcReadOp {
		stats.ErrorCount++
		fmt.Fprintf(os.Stderr, "Error reading %v\n", srcDir)
	}
	if !<-dstReadOp {
		stats.ErrorCount++
		fmt.Fprintf(os.Stderr, "Error reading %v\n", dstDir)
	}
	if stats.ErrorCount != 0 {
		out <- *stats
		return
	}

	srcSet := stringSet(srcDirnames)
	dstSet := stringSet(dstDirnames)
	inSourceDir := makeMapLookupTest(srcSet)
	inDestDir := makeMapLookupTest(dstSet)
	notInSourceDir := func(entry interface{}) bool {
		return !inSourceDir(entry)
	}

	srcNamesIter := iterable.StringArray(srcDirnames)
	dstNamesIter := iterable.StringArray(dstDirnames)
	inDst, notInDst := iterable.Partition(srcNamesIter, inDestDir)
	toBeDeletedNames := iterable.Filter(dstNamesIter, notInSourceDir)

	ops := new(outstandingOps)

	// TODO: sync contents
	fmt.Printf("Syncing dir %s -> %s\n", srcDir, dstDir)
	fmt.Println("src contents:", srcDirnames)
	fmt.Println("dst contents:", dstDirnames)

	fmt.Println("inDst contents:")
	for e := range inDst.Iter() {
		fmt.Println("  *", e)
	}

	for e := range notInDst.Iter() {
		go Copy(fmt.Sprintf("%s/%s", srcDir, e),
			fmt.Sprintf("%s/%s", dstDir, e),
			ops.new())
	}

	for e := range toBeDeletedNames.Iter() {
		go RemoveAll(fmt.Sprintf("%s/%s", dstDir, e), ops.new())
	}

	ops.wait(stats)

	fmt.Printf("sending stats for sync of dir %s\n", srcDir)
	out <- *stats
}
