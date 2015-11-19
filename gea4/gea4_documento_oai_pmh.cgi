#!/usr/bin/env perl

=head1 scelsi_oai_pmh.cgi

Essendo un CGI evita di caricare moduli impegnativi e anziche' caricare una configurazione esterna,
almeno inizialmente, usa delle costanti per connessione al db, costanti che renderemo variabili
ma da impostare in testa allo script.


=cut  

BEGIN {
   push(@INC, './lib');
}

use strict;
use warnings;
use utf8;
use CGI;
use Encode;
use CGI::Carp qw(fatalsToBrowser);
use DBI;
use Modern::Perl;
use URI::Escape;

use GEA4::DOCUMENTO;

use Data::Dumper;

my $GIT_COMMIT = q($Id$);

our $connect_info;

require('scelsi_config.pl');

my @METADATAFORMATS =    # qw/ead-san eac-san scons-san ricerca-san/
(
 {metadataPrefix=>'ead-san',
  schema=>'http://san.beniculturali.it/schema/eac-san.xsd',
  metadataNamespace=>'http://san.mibac.it/ead-san/',
 },
);

=head2 da fare
 {metadataPrefix=>'eac-san',
  schema=>'http://san.beniculturali.it/schema/eac-san.xsd',
  metadataNamespace=>'http://san.mibac.it/eac-san/',
 },
 {metadataPrefix=>'scons-san',
  schema=>'http://san.beniculturali.it/schema/scons-san.xsd',
  metadataNamespace=>'http://san.mibac.it/scons-san/',
 },
=cut


# my $dbh = DBI->connect("dbi:mysql:dbname=$connect_info->{dbname};host=$connect_info->{host};",$connect_info->{user},$connect_info->{password},$connect_info->{options}) || die("Cannot open db");
# my $dbh = DBI->connect('dbi:mysql:dbname=Scelsi;host=127.0.0.1;','root','managgia', {AutoCommit=>1, mysql_enable_utf8=>1}) || die("Cannot open db");

my    $dbh = DBI->connect ("dbi:CSV:", undef, undef, {
        f_schema         => undef,
        f_dir            => "./data",
        f_dir_search     => [],
        f_ext            => ".csv/r",
        f_lock           => 2,
        f_encoding       => "utf8",

        csv_eol          => "\r\n",
        csv_sep_char     => ",",
        csv_quote_char   => '"',
        csv_escape_char  => '"',
        csv_class        => "Text::CSV_XS",
        csv_null         => 1,
        csv_tables       => {
            info => { f_file => "info.csv" }
            },

        RaiseError       => 1,
        PrintError       => 1,
        FetchHashKeyName => "NAME_lc",
        }) or die $DBI::errstr;



my $q = CGI->new();
$q->charset('UTF-8');
my $res;

sub check {
    my ($val) = @_;
    return $val->[0] if ('ARRAY' eq ref $val);
    return $val ? $val : undef;
}

if ( 'GET' eq $ENV{REQUEST_METHOD}) {
    unless ('ARRAY' eq ref $q->param('verb')) {
	given( $q->param('verb') ) {
	    when (/ListMetadataFormats/) { $res = ListMetadataFormats(
								    $q,
								    check($q->param('identifier')), # optional
							       );
				       }
	    when (/ListIdentifiers/)     { $res = ListIdentifiers(
								$q, 
								check($q->param('resumptionToken')), # optional
								check($q->param('metadataPrefix')), 
								check($q->param('from')),            # optional
								check($q->param('until')),           # optional
								check($q->param('set')),         # optional
							       );}
	    when (/ListRecords/) { $res = ListRecords($q, 
						    check($q->param('resumptionToken')),         # optional
						    check($q->param('metadataPrefix')),
						    check($q->param('from')),                    # optional
						    check($q->param('until')),                   # optional
						    check($q->param('set')),                            # optional
						     );
			       }
	    when (/ListSets/)    { $res = ListSets($q, 
						   check($q->param('resumptionToken')),
						  );           # optional
			       }
	    when (/GetRecord/)   { $res = GetRecord($q, 
						  check($q->param('identifier')), 
						  check($q->param('metadataPrefix')),
						   );
			       }
	    when (/Identify/)    { $res=Identify($q); }
	    default { 
		$res = OAI_error($q, {code=>'badVerb', description=>"Illegal OAI verb"}, $_);
	    }
	}
	print $q->header(-type=>'text/xml', -'Powered-by' => 'NonSoLoSoft');
	print $res;
        exit;
    }
    print OAI_error($q, {code=>'badVerb', description=>"Illegal OAI verb"}, $_);
} else {
    die ("Only GET requests");
}


use constant {
    YEAR    =>5,
    MONTH   =>4,
    DAY     =>3,
    HOURS   =>2,
    MINUTES =>1,
    SECONDS =>0,
};

=head2 ListSets

Scelsi non ha set.


=cut

sub ListSets {
    my ($q, $resumptionToken) = @_;
    OAI_error($q, {code=>'noSetHierarchy', description=>'This repository does not support sets'}, 'ListSets');
}



=head2 Identify


=cut


sub Identify {
    my ($q) = @_;
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );
    my @ctime = gmtime((stat $0)[10]);
    my $cgi_date = sprintf ("%d-%02d-%dT%d:%d:%dZ", $ctime[YEAR]+1900, 1+$ctime[MONTH], $ctime[DAY],
		                             $ctime[HOURS]+1900, $ctime[MINUTES], $ctime[SECONDS],
		      );
    my $cgi_version = $GIT_COMMIT;
    qq(<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
 <responseDate>$now</responseDate>
 <request verb="Identify">$cgi_url</request>
 <Identify>
   <repositoryName>Scelsi OAI repository</repositoryName>
   <baseURL>$cgi_url</baseURL>
   <protocolVersion>2.0</protocolVersion>
   <adminEmail>mailto:davide.dovetta\@promemoriagroup.com</adminEmail>
   <earliestDatestamp>$cgi_date</earliestDatestamp>
   <deletedRecord>transitional</deletedRecord>
   <granularity>YYYY-MM-DD</granularity>
   <compression>gzip</compression>
   <compression>deflate</compression>
   <description>
    <oai-identifier xmlns="http://www.openarchives.org/OAI/2.0/oai-identifier" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai-identifier http://www.openarchives.org/OAI/2.0/oai-identifier.xsd">
      <scheme>oai</scheme>
      <repositoryIdentifier>GEA4Repository</repositoryIdentifier>
      <delimiter>:</delimiter>
      <sampleIdentifier>GEA4:IMG-00003950</sampleIdentifier>
    </oai-identifier>
   </description>
 </Identify>
</OAI-PMH>);
}

=head2 valid_date

Controlla che la data sia nel formato atteso, ritorna un'eccezione diversamente

=cut

sub valid_date {
    my ($d, $which) = @_;
    return $d if ($d =~ /\d{4}-\d{2}-\d{2}/);
}

use constant {
    PERM_ELEMENT_ID => 479,
    CA_OBJECTS_TABLE_NUM => 57,
    CA_ENTITIES_TABLE_NUM => 20,
};

my %METADATA_TABLE_NUM = (
			  'ead-san'=>CA_OBJECTS_TABLE_NUM,
			  'eac-san'=>CA_ENTITIES_TABLE_NUM,
			  'scons-san'=>CA_ENTITIES_TABLE_NUM,
);



=head2 ListIdentifiers


=cut


sub ListIdentifiers {
    my ($q, $resumptionToken, $metadataPrefix, $from, $until, $set) = @_;
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );
    unless ( grep {/^$metadataPrefix$/} (map {$_->{metadataPrefix}} @METADATAFORMATS) ) {
	return OAI_error($q, {code=>'cannotDisseminateFormat', description=>"$metadataPrefix is unknown metadata formats"}, 'ListIdentifiers');
    }
    if ((defined $from) && !valid_date($from)) {
	return OAI_error($q, {code=>'badArgument', description=>"The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax. (from: $from)"}, 'ListIdentifiers');
    }
    if ((defined $until) && !valid_date($until)) {
	return OAI_error($q, {code=>'badArgument', description=>"The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax. (until: $until)"}, 'ListIdentifiers');
    }
    $res = qq(<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
<responseDate>$now</responseDate>
<request metadataPrefix="$metadataPrefix" verb="ListIdentifiers">$cgi_url</request>
<ListIdentifiers>);

    my ($data) = CollectiveAccess::Scelsi::SQL::ListIdentifiers($dbh, $from, $until);
    if ($data) {
	for (@$data) {
	    $res .= qq(<header><identifier>$_->[0]</identifier><datestamp>$_->[1]</datestamp></header>\n)
	}
    } else {
	# 
    }

    #  $res .= qq(<resumptionToken expirationDate="2015-08-19T14:59:06Z" completeListSize="99735" cursor="0">1439992746628!50!99735!ead-san!san!0001-01-01!9999-12-31</resumptionToken>);
    $res .= qq(
</ListIdentifiers></OAI-PMH>
<!-- resumptionToken: $resumptionToken
     metadataPrefix: $metadataPrefix
     from: $from
     until: $until 
     set: $set
-->
);
    return $res;
}


=head2 ListMetadataFormats


=cut

sub ListMetadataFormats {
    my ($q, $identifier) = @_;
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );

    my $res = qq(<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
<responseDate>$now</responseDate>
<request verb="ListMetadataFormats">$cgi_url</request><ListMetadataFormats>);
    if ($identifier) {
	my $metadataPrefix;
	eval {
	    $metadataPrefix = CollectiveAccess::Scelsi::SQL::ListMetadataFormats($dbh, $identifier);
	};
	if ($@) {
	    if ('identifier not found' eq $@) {
		return OAI_error($q, {code=>'idDoesNotExist', description=>"The value of the identifier argument is unknown or illegal in this repository. ($identifier)"}, 'ListMetadataFormats');		
	    }
	    die ($@); # rialzo l'eccezione.
	}
        unless ($metadataPrefix) {
	    return OAI_error($q, {code=>'noMetadataFormats', description=>"There are no metadata formats available for the specified item. ($identifier)"}, 'ListMetadataFormats');
	}
	for (grep {$_->{metadataPrefix} =~ /^$metadataPrefix$/} @METADATAFORMATS) {
	    $res .= qq(<metadataFormat><metadataPrefix>$_->{metadataPrefix}</metadataPrefix><schema>$_->{schema}</schema><metadataNamespace>$_->{metadataNamespace}</metadataNamespace></metadataFormat>);
	}
    } else {
	for (@METADATAFORMATS) {
	    $res .= qq(<metadataFormat><metadataPrefix>$_->{metadataPrefix}</metadataPrefix><schema>$_->{schema}</schema><metadataNamespace>$_->{metadataNamespace}</metadataNamespace></metadataFormat>);
	}
    }
    $res .= q(</ListMetadataFormats></OAI-PMH>);
    return $res;
}

=head2 GetRecord


=cut

sub GetRecord {
    my ($q, $identifier, $metadataPrefix) = @_;
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );
    unless ( grep {/^$metadataPrefix$/} (map {$_->{metadataPrefix}} @METADATAFORMATS) ) {
	return OAI_error($q, {code=>'cannotDisseminateFormat', description=>"$metadataPrefix is unknown metadata formats"}, 'GetRecord');
    }

    if ($identifier) {
	if (my ($table_num, $object_id) = CollectiveAccess::Scelsi::SQL::GetRecord_check_identifier($dbh, $identifier)) {
	    my $res = qq(<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
<responseDate>$now</responseDate>
<request verb="GetRecord">$cgi_url</request><record>);
	    if (CollectiveAccess::Scelsi::SQL::Metadata_table_num($metadataPrefix) == $table_num) {
		if (CA_OBJECTS_TABLE_NUM == $table_num) {
		    ##
		    $res .= CollectiveAccess::Scelsi::SQL::CostruisciEAD($dbh, $object_id);
		    ##
		} elsif (CA_ENTITIES_TABLE_NUM == $table_num) {
		    # in seguito potrebbe discernere tra eac e scons
		    eval {
			$res .= CollectiveAccess::Scelsi::SQL::CostruisciEntita($dbh, $object_id, $metadataPrefix);
		    };
		    if ($@) {
			if ('metadataPrefix mismatch' eq $@) {
			    return OAI_error($q, {code=>'noMetadataFormats', description=>"There are no metadata formats available for the specified item. ($identifier) table_num: $table_num, object_id: $object_id"}, 'GetRercord');
			}
		    }
		}
	    } else {
		return OAI_error($q, {code=>'noMetadataFormats', description=>"There are no metadata formats available for the specified item. ($identifier) table_num: $table_num, object_id: $object_id"}, 'GetRercord');
	    }

	    $res .= q(</record></OAI-PMH>);
	    return $res;
	} else {
	    return OAI_error($q, {code=>'idDoesNotExist', description=>"The value of the identifier argument is unknown or illegal in this repository. ($identifier)"}, 'GetRecord');
	}
    }
    return OAI_error($q, {code=>'badArgument', description=>"The request includes illegal arguments or is missing required arguments."}, 'GetRecord');
}

=head2 ListRecords

Una prima implementazione estrae tutti gli object_id e fa un loop sul codice creato per GetRercord.
In seguito si puo' cambiare l'implementazione riducendo le query.

=cut

sub ListRecords {
    my ($q, $resumptionToken, $metadataPrefix, $from, $until, $set) = @_;
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );
    unless ( grep {/^$metadataPrefix$/} (map {$_->{metadataPrefix}} @METADATAFORMATS) ) {
	return OAI_error($q, {code=>'cannotDisseminateFormat', description=>"$metadataPrefix is unknown metadata formats"}, 'ListRecords');
    }
    if ((defined $from) && !valid_date($from)) {
	return OAI_error($q, {code=>'badArgument', description=>"The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax. (from: $from)"}, 'ListRecords');
    }
    if ((defined $until) && !valid_date($until)) {
	return OAI_error($q, {code=>'badArgument', description=>"The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax. (until: $until)"}, 'ListRecords');
    }

    # su questo metodo c'e' da valutare se implementare il resumptionToken:
    #  i dati potrebbero diventare troppi da estrarre ed esportare in una singola request.
    #  ma probabilmente non per Scelsi.
    
    my $eads = GEA4::FONDO::ListRecords($dbh, $resumptionToken, $metadataPrefix, $from, $until);
    my $res = qq(<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
<responseDate>$now</responseDate>
<request verb="ListRecords">$cgi_url</request><ListRecords>);

    if (defined $eads && (scalar @$eads)) {
	$res .= join "\n", map {"<record>$_</record>"} @$eads;
    } else {
	# no result
    }
    $res .= q(</ListRecords></OAI-PMH>);
    return $res;
}

sub OAI_error {
    my ($q, $causa, $verb, $arg) = @_;
    # oppure HTTP_HOST anziché SERVER_NAME
    my $cgi_url = 'http://' . $ENV{SERVER_NAME} . $ENV{REQUEST_URI};
    $cgi_url =~ s/&/&amp;/sg;
    my @time = gmtime(time);
    my $now = sprintf ("%d-%02d-%dT%d:%d:%dZ", $time[YEAR]+1900, 1+$time[MONTH], $time[DAY],
		                             $time[HOURS]+1900, $time[MINUTES], $time[SECONDS],
		      );

    qq(<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>$now</responseDate> 
  <request verb="$verb">$cgi_url</request>
  <error code="$causa->{code}">$causa->{description}</error>
</OAI-PMH>);
}

__END__

