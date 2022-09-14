use strict;
use warnings;

my $request=$ARGV[0];
if($request eq "target_smsbc"){
	my $dir_source=$ARGV[1];
	my $dir_data=$ARGV[2];
	my $target_date=$ARGV[3];
	my $max_send=$ARGV[4];
	my $file_name=$ARGV[5];
# get source file
	# cp ./source/[file_name].txt ./data
	`cp $dir_source"/"$file_name".txt" $dir_data`;

	# get msisdn
	# head -n[max_send] ./data/[file_name].txt > ./data/[date]_smsbc_temp.txt
	`head -n$max_send $dir_data"/"$file_name".txt" > $dir_data"/"$target_date"_smsbc_temp.txt"`;

	# cut source
	# sed 1,[max_send]d ./data/[file_name].txt > ./data/[file_name]_new.txt
	`sed 1,${max_send}d $dir_data"/"$file_name".txt" > $dir_data"/"$file_name"_new.txt"`;

        # re-add source
        `/usr/bin/cat $dir_data"/"$target_date"_smsbc_temp.txt" >> $dir_data"/"$file_name"_new.txt"`;

	# overwrite ori
	# mv ./data/[file_name]_new.txt ./source/[file_name].txt
	`mv $dir_data"/"$file_name"_new.txt" $dir_source"/"$file_name".txt"`;
	
	# remove source
	# rm ./data/[file_name].txt
	`rm $dir_data"/"$file_name".txt"`;
        
}elsif($request eq "sort_blacklist"){
    my $check_done = `tail -1 blacklist_temp.txt | grep done`;
    chomp $check_done;
       
    if( $check_done ne "done" ){
        system("/usr/bin/rm","blacklist_temp.txt");
        die "Blacklist querying is interupted";
    }
       
    my $check_error = `grep ERROR blacklist_temp.txt`;
    chomp $check_error;
       
    if( $check_error ne "" ){
        system("/usr/bin/rm","blacklist_temp.txt");
        die "Error on retrieving whitelist";
    }
		
	#`/usr/bin/dos2unix -437 ./blacklist_temp.txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > ./data/blacklist.txt`;
        `/usr/bin/cat ./blacklist_temp.txt | /usr/bin/dos2unix -437 | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > ./data/blacklist.txt`;
     system("/usr/bin/rm","blacklist_temp.txt"); 

}elsif($request eq "sort_compare_blacklist"){
	my $dir_data=$ARGV[1];
	my $target_date=$ARGV[2];
	my $blacklist_file=$ARGV[3];
	my $sub_file_id=$ARGV[4];
	#sort the input 1
	# /usr/bin/dos2unix -437 ./data/[date]_smsbc_temp.txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > ./data/[date]_smsbc_temp_sorted_temp1.txt
	#`/usr/bin/dos2unix -437 $dir_data"/"$target_date"_smsbc_temp.txt" | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$target_date"_smsbc_temp_sorted_temp1.txt"`;
        `/usr/bin/cat $dir_data"/"$target_date"_smsbc_temp.txt" | /usr/bin/dos2unix -437 | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$target_date"_smsbc_temp_sorted_temp1.txt"`;
	
        #compare 
	# /usr/bin/comm -23 ./data/[date]_smsbc_temp_sorted_temp1.txt ./data/[blacklist_file].txt > ./data/[date]_[sub_file_id]_sorted.txt
	`/usr/bin/comm -23 $dir_data"/"$target_date"_smsbc_temp_sorted_temp1.txt" $dir_data"/"$blacklist_file".txt" > $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt"`;

	#delete temp file
	# /usr/bin/rm ./data/[date]_smsbc_temp_sorted_temp1.txt
	`/usr/bin/rm $dir_data"/"$target_date"_smsbc_temp_sorted_temp1.txt"`;

}elsif($request eq "sort_compare_history"){
	my $dir_data=$ARGV[1];
	my $target_date=$ARGV[2];
	my $history_file=$ARGV[3];
	my $sub_file_id=$ARGV[4];
	#sort the input 1
	# /usr/bin/dos2unix -437 ./data/[date]_[sub_file_id]_sorted.txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > ./data/[date]_sorted_temp1.txt
	#`/usr/bin/dos2unix -437 $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt" | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$target_date"_sorted_temp1.txt"`;
        `/usr/bin/cat $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt" | /usr/bin/dos2unix -437 | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$target_date"_sorted_temp1.txt"`;
	
        #sort the input 2
	# /usr/bin/dos2unix -437 ./data/[history].txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > ./data/blacklist_sorted_temp2.txt
	#`/usr/bin/dos2unix -437 $dir_data"/"$history_file".txt" | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$history_file"_sorted_temp2.txt"`;
        `/usr/bin/cat $dir_data"/"$history_file".txt" | /usr/bin/dos2unix -437 | /usr/bin/sed 's/ //g' | /usr/bin/sort -u > $dir_data"/"$history_file"_sorted_temp2.txt"`;

	#compare 
	# /usr/bin/comm -23 ./data/[date]_sorted_temp1.txt ./data/history_file_sorted_temp2.txt > ./data/[date]_[sub_file_id]_sorted.txt
	`/usr/bin/comm -23 $dir_data"/"$target_date"_sorted_temp1.txt" $dir_data"/"$history_file"_sorted_temp2.txt" > $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt"`;
	
	#delete temp file
	# /usr/bin/rm ./data/[date]_sorted_temp1.txt
	`/usr/bin/rm $dir_data"/"$target_date"_sorted_temp1.txt"`;
	# /usr/bin/rm ./data/blacklist_sorted_temp2.txt
	`/usr/bin/rm $dir_data"/"$history_file"_sorted_temp2.txt"`; 

}elsif($request eq "append_file"){
	my $dir_data=$ARGV[1];
	my $target_date=$ARGV[2];
	my $sub_file_id=$ARGV[3];
	# append file
	# /usr/bin/cat ./data/[date]_[sub_file_id]_sorted.txt >> ./data/[date]_sub_file_id]_sorted_final.txt
	`/usr/bin/cat $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt" >> $dir_data"/"$target_date"_"$sub_file_id"_sorted_final.txt"`;
	
	#sort
	# /usr/bin/dos2unix -437 ./dir_data/[target_date]_[sub_file_id]_sorted_final.txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -o| /usr/bin/sort -u > ./dir_data/[target_date]_[sub_file_id]_sorted.txt`;
	#`/usr/bin/dos2unix -437 $dir_data"/"$target_date"_"$sub_file_id"_sorted_final.txt | /usr/bin/sed 's/ //g' | /usr/bin/sort -o| /usr/bin/sort -u > $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt`;

	# /usr/bin/cp ./data/[date]_sub_file_id]_sorted.txt ./data/[date]_sub_file_id]_sorted_final.txt
	`/usr/bin/cp $dir_data"/"$target_date"_"$sub_file_id"_sorted_final.txt" $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt"`;
	
}elsif($request eq "cut_file"){
	my $dir_source=$ARGV[1];
	my $dir_data=$ARGV[2];
	my $target_date=$ARGV[3];
	my $file_name=$ARGV[4];
	my $sub_file_id=$ARGV[5];
	my $cut_line=$ARGV[6];
    # append to source file
	# head -n[cut_line] ./data/[date]_[sub_file_id]_sorted.txt >> ./source/[file_name].txt
	#`head -n$cut_line $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt" >> $dir_source"/"$file_name".txt"`;

	# cut file sorted_file
	# sed 1,[cut_file]d ./data/[date]_[sub_file_id]_sorted_final.txt > ./data/[date]_[sub_file_id]_sorted.txt
	`sed 1,${cut_line}d $dir_data"/"$target_date"_"$sub_file_id"_sorted_final.txt" > $dir_data"/"$target_date"_"$sub_file_id"_sorted.txt"`;

}
