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

func main() {
	flag.Parse()
	if (flag.NArg() != 2) {
		os.Stderr.WriteString("Usage: hblsync <src_dir> <dst_dir>\n")
		os.Exit(1)
	}

	srcArg := flag.Arg(0)
	dstArg := flag.Arg(1)

	srcFile := openDirectoryOrDie("source", srcArg)
	dstFile := openDirectoryOrDie("destination", dstArg)

	fmt.Printf("src: " + srcFile.Name() + "\n")
	fmt.Printf("dst: " + dstFile.Name() + "\n")
}

