#!/usr/bin/perl

#sort the input 1
`/usr/bin/cat $ARGV[0]"/"$ARGV[1]".txt" | /usr/bin/dos2unix -437  | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $ARGV[0]"/"$ARGV[1]"_sorted_temp1.txt"`;

#sort the input 2
`/usr/bin/cat $ARGV[0]"/"$ARGV[2]".txt" | /usr/bin/dos2unix -437  | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $ARGV[0]"/"$ARGV[2]"_sorted_temp2.txt"`;

#compare 
`/usr/bin/comm -23 $ARGV[0]"/"$ARGV[1]"_sorted_temp1.txt" $ARGV[0]"/"$ARGV[2]"_sorted_temp2.txt" > $ARGV[0]"/"$ARGV[3]"_sorted.txt"`;

#delete temp file
`/usr/bin/rm $ARGV[0]"/"$ARGV[1]"_sorted_temp1.txt"`;
`/usr/bin/rm $ARGV[0]"/"$ARGV[2]"_sorted_temp2.txt"`;
