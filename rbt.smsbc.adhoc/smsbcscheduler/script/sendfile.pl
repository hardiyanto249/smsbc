#!/usr/bin/perl

# send file
`/usr/bin/scp $ARGV[0]".txt" rbtuser@$ARGV[1]:/app1/rbtuser/rbt/push/src/`;
