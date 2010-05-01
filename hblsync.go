// hblsync -- High Bandwidth & Latency directory sync tool
//
// Copyright 2010 Brad Fitzpatrick
// brad@danga.com
//
// Usage: hblsync <srcdir> <dstdir>
//

package main

import "fmt"
import "flag"
import "os"

func openDirectoryOrDie(which string, dirName string) *os.File {
	file, e := os.Open(dirName, os.O_RDONLY, 0)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error opening %s directory: %s\n",
			which, e.String())
		os.Exit(1)
	}
	stat, e := file.Stat()
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
	return file
}

type SyncStats struct {
	ErrorCount int
	DirsCreated int
	DirsDeleted int
	DirsGood int
	FilesCreated int
	FilesReplaced int
	FilesDeleted int
	FilesGood int
}

func readDirnames(dir *os.File, outNames *[]string, ok chan bool) {
	names, err := dir.Readdirnames(-1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading dirnames: %v\n", err)
		ok <- false
	} else {
		*outNames = names
		ok <- true
	}
}

func SyncDirectories(srcDir *os.File, dstDir *os.File, out chan SyncStats) {
	stats := new(SyncStats)
	var srcDirnames []string
	var dstDirnames []string
	srcReadOp := make(chan bool)
	dstReadOp := make(chan bool)
	go readDirnames(srcDir, &srcDirnames, srcReadOp)
	go readDirnames(dstDir, &dstDirnames, dstReadOp)
	if !<-srcReadOp {
		stats.ErrorCount++
		fmt.Fprintf(os.Stderr, "Error reading %v", srcDir)
	}
	if !<-dstReadOp {
		stats.ErrorCount++
		fmt.Fprintf(os.Stderr, "Error reading %v", dstDir)
	}
	if stats.ErrorCount != 0 {
		out <- *stats
		return
	}

	// TODO: sync contents
	fmt.Println("src contents:", srcDirnames)
	fmt.Println("dst contents:", dstDirnames)

	out <- *stats
}

func main() {
	flag.Parse()
	if (flag.NArg() != 2) {
		os.Stderr.WriteString("Usage: hblsync <src_dir> <dst_dir>\n")
		os.Exit(1)
	}
	srcFile := openDirectoryOrDie("source", flag.Arg(0))
	dstFile := openDirectoryOrDie("destination", flag.Arg(1))

	ch := make(chan SyncStats)
	go SyncDirectories(srcFile, dstFile, ch)
	results := <-ch

	fmt.Println("Results:", results)
}

