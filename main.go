// gotsync: Go Tree Syncer
//
// Massively parallel filesystem tree syncer.
//
// Assumes filesystem stat() operations are slow (such as syncing a
// large tree from local disk to a remote NFS server, where dentries
// need to be revalidated remotely...) and instead does everything it
// can at once.
//
// Copyright 2010 Brad Fitzpatrick
// brad@danga.com
//
// Usage: gotsync <srcdir> <dstdir>
//

package main

import "flag"
import "fmt"
import "os"
import gotsync "./gotsync"

var verbose *bool = flag.Bool("v", false, "verbose")
var delete *bool = flag.Bool("delete", false, "delete mode (takes 1 argument)")

func usage() {
	fmt.Fprint(os.Stderr, "usage: gotsync <src_dir> <dst_dir>\n")
	fmt.Fprint(os.Stderr, "       gotsync --delete <dir>\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Parse()
	resultChan := make(chan gotsync.SyncStats)

	syncer := gotsync.New()

	// Delete mode
	if (*delete) {
		if (flag.NArg() != 1) { usage() }
		go syncer.RemoveAll(flag.Arg(0), resultChan)
		fmt.Print(<-resultChan)
		return
	}

	// Sync mode
	if (flag.NArg() != 2) {
		usage();
	}

	srcDir, dstDir := flag.Arg(0), flag.Arg(1)
	permission := checkSourceDirectory(srcDir)
	checkOrMakeDestinationDirectory(dstDir, permission)

	go syncer.SyncDirectories(srcDir, dstDir, resultChan)
	fmt.Print(<-resultChan)
}

func checkSourceDirectory(dirName string) int {
	stat, e := os.Stat(dirName)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error checking source directory: %v\n", e)
		os.Exit(1)
	}
	if !stat.IsDirectory() {
		fmt.Fprintf(os.Stderr, "Source directory (%s) isn't a directory.",
			dirName)
		os.Exit(1)
	}
	return stat.Permission()
}

func checkOrMakeDestinationDirectory(dirName string, permission int) {
        stat, e := os.Stat(dirName)
	if e == nil {
		if stat.IsDirectory() {
			return
		}
		fmt.Fprintf(os.Stderr, "Target directory (%s) exists but " +
			"isn't a directory.\n", dirName)
		os.Exit(1)
	}

	err := os.Mkdir(dirName, permission)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create destination " +
			"directory: %v\n", err)
                os.Exit(1)
	}
}

