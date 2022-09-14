#!/usr/bin/perl
        
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
		
        system( "/usr/bin/sort","blacklist_temp.txt","-o","blacklist.txt" );
        system("/usr/bin/mv","blacklist.txt","./data/blacklist.txt"); 
        system("/usr/bin/rm","blacklist_temp.txt");
