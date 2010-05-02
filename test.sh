#!/bin/bash

set -x
#set -e

#rm -rf /tmp/hblsync-a
#mkdir -p /tmp/hblsync-a/foo/bar/quux
#echo "barsidefile contents" > /tmp/hblsync-a/foo/barsidefile
#touch /tmp/hblsync-a/foo/bar/barinfile-empty
#echo "I am somefile" > /tmp/hblsync-a/foo/bar/quux/somefile
#echo "I am somefile2" > /tmp/hblsync-a/foo/bar/quux/somefile2

rm -rf /tmp/hblsync-b
mkdir /tmp/hblsync-b
./hblsync /tmp/hblsync-a /tmp/hblsync-b
