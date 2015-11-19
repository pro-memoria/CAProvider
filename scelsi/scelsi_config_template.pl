#!/usr/bin/env perl

use strict;
use warnings;

our $connect_info = {
		   dbname=>'!dbname',
		   host=>'!hostIps',
		   user=>'scegli utente',
		   password=>'inserisci la password',
		   options => {
			       AutoCommit=>1,
			       mysql_enable_utf8=>1
			       },
};

our $resolver = {
		 on_success=>'!urlCa',
		 on_error=>'!urlCa',
		 utente=>'!administrator',
		 password=>'!secret',
};

1;
