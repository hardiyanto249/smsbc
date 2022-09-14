#!/usr/bin/perl

# get source file
`cp $ARGV[4]"/"$ARGV[1]".txt" $ARGV[2]`;

# get msisdn
`head -n$ARGV[0] $ARGV[2]"/"$ARGV[1]".txt" > $ARGV[2]"/"$ARGV[3]"_smsbc_temp.txt"`;

# cut source
`sed 1,$ARGV[0]d $ARGV[2]"/"$ARGV[1]".txt" > $ARGV[2]"/"$ARGV[1]"_new.txt"`;

# add source
`cat $ARGV[2]"/"$ARGV[3]"_smsbc_temp.txt" >> $ARGV[2]"/"$ARGV[1]"_new.txt"`;
 
# overwrite ori
`mv $ARGV[2]"/"$ARGV[1]"_new.txt" $ARGV[4]"/"$ARGV[1]".txt"`;
`rm $ARGV[2]"/"$ARGV[1]".txt"`;
