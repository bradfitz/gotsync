#!/bin/bash

set -x

rm -rf /tmp/hblsync-a
mkdir -p /tmp/hblsync-a/foo/bar/quux
echo "barsidefile contents" > /tmp/hblsync-a/foo/barsidefile
ln -s LinkTarget1 /tmp/hblsync-a/link1
touch /tmp/hblsync-a/foo/bar/barinfile-empty
echo "I am somefile" > /tmp/hblsync-a/foo/bar/quux/somefile
echo "I am somefile2" > /tmp/hblsync-a/foo/bar/quux/somefile2
ln -s LinkTarget2 /tmp/hblsync-a/foo/bar/quux

rm -rf /tmp/hblsync-b
mkdir /tmp/hblsync-b
./gotsync -v /tmp/hblsync-a /tmp/hblsync-b
