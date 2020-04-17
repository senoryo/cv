#!/usr/bin/perl
use strict;

use LWP::Simple;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;


my $USAGE = "Usage: $0 [--git-commit] <output-dir>";

my $gitCommit = 0;
if ($ARGV[0] =~ m/^-/) {
    $ARGV[0] eq '--git-commit' or die "Invalid option '$ARGV[0]'\n$USAGE\n";
    shift @ARGV;
    $gitCommit = 1; 
}

my $dir = shift @ARGV or die "$USAGE\n";


#US FORMAT:
#date,county,state,fips,cases,deaths
#2020-01-21,Snohomish,Washington,53061,1,0


#GLOBAL FORMAT:
#Date,Country/Region,Province/State,Lat,Long,Confirmed,Recovered,Deaths
#2020-01-22,Afghanistan,,33,65,0,0,0


my %dataSources = 
    (

     Global => 
     {
	 url => 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv',
	 header => 'Date,Country/Region,Province/State,Lat,Long,Confirmed,Recovered,Deaths',
	 createRecord => sub {
	     my ($date,$nation,$state,$lat,$long,$cases,$recovered,$deaths) = split ',', $_[0];
	     return {cases=>$cases, date=>$date, nation=>$nation, state=>$state, county=>''};	 
	 }
     },

     US => 
     {
	 url => 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv',
	 header => 'date,county,state,fips,cases,deaths',
	 createRecord => sub {
	     my ($date,$county,$state,$fips,$cases,$deaths) = split ',', $_[0];
	     #return undef if ($county eq 'New York City'); #REMOVE ME
	     
	     return {cases=>$cases, date=>$date, nation=>'US', state=>$state, county=>$county};
	 }
     }
     
    );

my %data = ();
my %dates = ();
my $dateCount = 0;


while (my ($sourceName,$s) = each %dataSources) {
    #next if ($sourceName eq 'Global'); #remove me
    
    my $data = get $s->{url} or die "Failed to open URL '$s->{url}'\n$!\n";

    open DATA, "<", \$data;
    
    my $header = <DATA>;
    chomp $header;
    $header =~ s/^\s+//g;
    $header =~ s/\s+$//g;
    
    my @header = split ',', $header;
    $header eq $s->{header} or die "Expected header '$s->{header}' for data source '$sourceName'\n Got '$header'\n";

    while (my $line = <DATA>) {
	chomp $line;
	my $record = $s->{createRecord}($line);
	next unless (defined $record);
	    
	my $locID = "$record->{nation},$record->{state},$record->{county}"; #unique locationID (but also convenient "nation,state,county"
	my $date = $record->{date};
	$data{$locID}{$date} = $record;
	
	if (!exists($dates{$date})) {
	    $dates{$date} = ++$dateCount;
	}
    }
    close DATA;
}


my @dates = sort keys %dates;

my %batchDates = ();
my $batchNumDays = 7;

my @focusAreasRegexes = ('New York,New York City','New York,Nassau', 'Japan');
my @focusAreas = ();
my %focusAreas = ();
my $focusAreasStartDate = undef;

sub log2 {
    return log($_[0])/log(2);
}

#enrich with deltas, fill in blank dates, set focus areas and focus area start date
while (my ($locid,$r) = each %data) {
    my $prev = undef;
    my $batchDay = 0;
    my $batchHead = undef;
    my $casesDenom = undef;
    
    for my $focusAreaRegex (@focusAreasRegexes) {
	if ($locid =~ m/$focusAreaRegex/) {
	    push @focusAreas, $locid;
	    $focusAreas{$locid} = 1;
	    last;
	}
    }
    
    for my $date (sort {$b cmp $a} @dates) { #reverse order
	my $curr = undef;
	if (!exists($r->{$date})) {
	    $r->{$date} = {_date=>$date,cases=>0,deltaCases=>0};
	}
	
	$curr = $r->{$date};

	if ($curr->{cases} > 0) {
	    $curr->{logCases} = log2($curr->{cases});
	}

	if (exists($focusAreas{$locid}) &&
	    !defined($focusAreasStartDate) &&
	    $curr->{cases} <= 10) {
	    $focusAreasStartDate = $date;
	}
	
	
	#print "CURR [$date]: ";print Dumper($curr);
	
	## 1-Day delta ##
	if (defined $prev) {
	    $prev->{deltaCases} = $prev->{cases} - $curr->{cases};
	    if (exists($curr->{logCases}) && exists($prev->{logCases})) {
		$prev->{deltaLogCases} = $prev->{logCases} - $curr->{logCases};
	    }
	}
	$prev = $curr;
	
	#print "  PREV [$date]: ";print Dumper($prev);
	
	## N-Days Delta (aka Batch Delta) ##
	++$batchDay;
	$batchDay = 1 if($batchDay > $batchNumDays);
	
	if ($batchDay == 1) { 
	    if (defined $batchHead) {
		$batchHead->{batchDeltaCases} = $batchHead->{cases} - $curr->{cases};
		
		if (!defined($casesDenom)) {
		    $casesDenom = $batchHead->{batchDeltaCases};
		}
		
		if ($casesDenom > 0) {
		    $batchHead->{batchDeltaNormalCases} = 100 *($batchHead->{batchDeltaCases} / $casesDenom);
		}
		else {
		    $batchHead->{batchDeltaNormalCases} = 100;
		}
	    }
	    
	    $batchHead = $curr;
	    $batchDates{$date} = 1;
	}
    }
}

my @batchDates = sort keys %batchDates;

#calc double speed
while (my ($locid,$r) = each %data) {
    for my $outDate (sort {$b cmp $a} @dates) { # reverse order - outer loop
	my $curr = $r->{$outDate};

	if (defined $curr->{deltaLogCases}) {
	    my $runningLogDelta = $curr->{deltaLogCases};
	    my $doubleSpeed = 1;
	    
	    if ($runningLogDelta < 1) {
		for my $inDate (sort {$b cmp $a} @dates) { #reverse order - inner loop
		    next if ($inDate ge $outDate);
		    
		    my $temp = $r->{$inDate};
		    if (defined $temp->{deltaLogCases}) {
			if (int($runningLogDelta + $temp->{deltaLogCases} + .0001) >= 1) {
			    if ($temp->{deltaLogCases} > 0) {
				$doubleSpeed += (1-$runningLogDelta)/$temp->{deltaLogCases};
			    }
			    last;
			}
			++$doubleSpeed;
			$runningLogDelta += $temp->{deltaLogCases};
		    }
		}
	    }
	    $curr->{doubleSpeed} = $doubleSpeed;
	}
    }
}

#print STDERR Dumper(\%data);die;#remove me



#OUTPUT:


sub printCsvData {
    my $data = shift;
    my $dates = shift;
    my $dir = shift;
    my $filePrefix = shift;
    my $stat = shift;
    my $sortByStat = shift;
    
    my $filepath = "$dir/${filePrefix}.csv";
    
    open FILE, ">$filepath" or die "Failed to open '$filepath' for writing.\n$!\n";
    
    print FILE "nation,state,county";
    for my $date (@$dates) {
	print FILE ",$date";
    }
    print FILE "\n";
    
    
    for my $locid (sort {$data->{$b}{$dates->[-1]}{cases} <=> $data->{$a}{$dates->[-1]}{cases}} keys %$data) {
	print FILE "$locid";
	for my $date (@$dates) {
	    print FILE ",$data->{$locid}{$date}{$stat}";
	}
	print FILE "\n";
    }
    
    close FILE;    
}

sub printCsvFocusAreas {
    my $data = shift;
    my $dates = shift;
    my $dir = shift;
    my $filePrefix = shift;
    my $stat = shift;
    my $focusAreas = shift; #array
    my $focusAreasStartDate = shift;
    
    my $filepath = "$dir/${filePrefix}-Focus.csv";

    open FILE, ">$filepath" or die "Failed to open '$filepath' for writing\n$!\n";
    
    print FILE "date";
    for my $locid (@$focusAreas) {
	my $formattedLOCID = $locid;
	$formattedLOCID =~ s/\,/-/g;
	$formattedLOCID =~ s/^-//;
	$formattedLOCID =~ s/-$//;
	print FILE ",$formattedLOCID";
    }
    print FILE "\n";

    #print "'$focusAreasStartDate'\n";
    for my $date (@$dates) {
	next if($date le $focusAreasStartDate);
	
	print FILE "$date";
	for my $locid (@$focusAreas) {
	    print FILE ",$data{$locid}{$date}{$stat}";	
	}
	print FILE "\n";
    }
    close FILE;    
}

printCsvData(\%data,\@dates,     $dir,'Total',                        'cases',          'cases');
printCsvData(\%data,\@dates,     $dir,'Delta',                        'deltaCases',     'cases');
printCsvData(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",      'batchDeltaCases','cases');
printCsvData(\%data,\@batchDates,$dir,"${batchNumDays}DayDeltaNormal",'batchDeltaNormalCases','cases');
printCsvData(\%data,\@dates,     $dir,'DoubleSpeed',                  'doubleSpeed',    'cases');

printCsvFocusAreas(\%data,\@dates,     $dir,'Total',                  'cases',          \@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@dates,     $dir,'Delta',                  'deltaCases',     \@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",'batchDeltaCases',\@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@batchDates,$dir,"${batchNumDays}DayDeltaNormal",'batchDeltaNormalCases',\@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@dates,     $dir,'DoubleSpeed',            'doubleSpeed',    \@focusAreas,$focusAreasStartDate);

if ($gitCommit) {
    my $time = `date`;
    chomp $time;

    my $cmd = '';
    
    $cmd = "git commit -a -m 'Updated data at $time'";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";
    
    $cmd = "git push -u origin master";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";    
}
