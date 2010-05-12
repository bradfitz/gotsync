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
//
// Usage: gotsync [-v] <srcdir> <dstdir>
//

package main

import "http"
import _ "http/pprof"
import "flag"
import "fmt"
import "os"
import gotsync "./gotsync"

var verbose *bool = flag.Bool("v", false, "verbose")
var android *bool = flag.Bool("android", false, "Android mode; skip .repo and .git dirs")
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
		
	go http.ListenAndServe(":12345", nil) 
	
	syncer := gotsync.New()
	syncer.Verbose = *verbose
	syncer.AndroidMode = *android

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

	results := <-resultChan
	fmt.Print(results)
	os.Exit(results.ErrorCount)
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

