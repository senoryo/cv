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

	$s->{maxDate} = $date if(!defined($s->{maxDate}) || ($date ge $s->{maxDate}));
	
	$data{$locID}{$date} = $record;
	
	if (!exists($dates{$date})) {
	    $dates{$date} = ++$dateCount;
	}
    }
    close DATA;
}

my $minOfTheMaxDates = undef;
while (my ($sourceName,$s) = each %dataSources) {
    if (defined($minOfTheMaxDates)) {
	$minOfTheMaxDates = $s->{maxDate} if($s->{maxDate} lt $minOfTheMaxDates);
    }
    else {
	$minOfTheMaxDates = $s->{maxDate};
    }
}

for my $date (keys %dates) {
    delete $dates{$date} if($date gt $minOfTheMaxDates);
}

my @dates = sort keys %dates;

my %batchDates = ();
my $batchNumDays = 7;

my @focuses = 
    (
     {
	 name => 'NY',
	 regexes => ['New York,New York City','New York,Nassau','Chicago','Maricopa','Singapore','Israel','Hong Kong'],
	 locids => [],
	 locidsHash => {},
	 startDate => ''
     },
     
     {
	 name => 'Asia',
	 regexes => ['Japan', 'Hubei', 'Hong Kong'],
	 locids => [],
	 locidsHash => {},
	 startDate => ''     
     }

    );
     
sub log2 {
    return log($_[0])/log(2);
}

my %recentAccelerations = ();

#enrich with deltas, fill in blank dates, set focus locids, set accelerating locids
while (my ($locid,$r) = each %data) {
    my $prev = undef;
    
    my $batchDay = 0;
    my $batchHead = undef;

    my $deltaMax = 0;
    my $batchMax = 0;
    
    my $mostRecentBatchDelta = undef;
    
    for my $f (@focuses) {
	for my $regex (@{$f->{regexes}}) {
	    if ($locid =~ m/$regex/) {
		push @{$f->{locids}}, $locid;
		$f->{locidsHash}{$locid} = 1;
		last;
	    }
	}
    }
    
    for my $date (sort {$b cmp $a} @dates) { #reverse order
	my $curr = undef;
	if (!exists($r->{$date})) {
	    $r->{$date} = {_fake=>1,date=>$date,cases=>0,deltaCases=>0};
	}
	
	$curr = $r->{$date};

	if ($curr->{cases} > 0) {
	    $curr->{logCases} = log2($curr->{cases});
	}
	
	for my $f (@focuses) {
	    if (exists($f->{locidsHash}{$locid}) &&
		($f->{startDate} eq '') &&
		$curr->{cases} <= 10) {
		
		$f->{startDate} = $date;
	    }
	}
	    
	
	#print "CURR [$date]: ";print Dumper($curr);
	
	## 1-Day delta ##
	if (defined $prev) {
	    $prev->{deltaCases} = $prev->{cases} - $curr->{cases};
	    $deltaMax = $prev->{deltaCases} if($prev->{deltaCases} > $deltaMax);
	    
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
		$batchMax = $batchHead->{batchDeltaCases} if($batchHead->{batchDeltaCases} > $batchMax);
		
		if (!defined($mostRecentBatchDelta)) {
		    $mostRecentBatchDelta = $batchHead->{batchDeltaCases};
		}
		elsif (!exists($recentAccelerations{$locid})) {
		    my $diff = $mostRecentBatchDelta - $batchHead->{batchDeltaCases};
		    if ($diff < 100 && $diff > -100) {
			$recentAccelerations{$locid} = 0; #di minimis, make it 0
		    }
		    elsif ($batchHead->{batchDeltaCases} == 0) {
			$recentAccelerations{$locid} = 10; #i.e. 1,000% -- arbitrarily high
		    }
		    else {
			$recentAccelerations{$locid} = $diff / $batchHead->{batchDeltaCases};			
		    }
		}
	    }
	    
	    $batchHead = $curr;
	    $batchDates{$date} = 1;
	}
    }
    
    #set normalized deltas and batchDeltas
    if ($deltaMax > 0 && $batchMax > 0) {
	$deltaMax *= 1.05; #extra headroom for charting, so that max point doesnt hit top of chart
	$batchMax *= 1.05;
	
	while (my ($k,$v) = each %$r) {
	    if (defined($v->{deltaCases})) {
		$v->{deltaNormalCases} = 100* $v->{deltaCases} / $deltaMax;
	    }
	    if (defined($v->{batchDeltaCases})) {
		$v->{batchDeltaNormalCases} = 100* $v->{batchDeltaCases} / $batchMax;
	    } 
	}
    }
}

my @batchDates = sort keys %batchDates;


#create hotspots focus areas based on recent accelerations
my $numAccelerations = 0;
my $hotspotFocus =
{
    name => 'Hotspots',
    locids => [],
    locidsHash => {},
    startDate => ''     
};
push @focuses, $hotspotFocus;

for my $locid (sort {$recentAccelerations{$b} <=> $recentAccelerations{$a}} keys %recentAccelerations) {
    next if ($data{$locid}{$dates[-1]}{batchDeltaCases} < 3000); #skip small cases
    
    last if (++$numAccelerations > 15);
    push @{$hotspotFocus->{locids}}, $locid;
    $hotspotFocus->{locidsHash}{$locid} = 1;
}

#populate focuses start dates
while (my ($locid,$r) = each %data) {
    for my $date (sort @dates) {
	for my $f (@focuses) {
	    if (exists($f->{locidsHash}{$locid}) &&
		($f->{startDate} eq '') &&
		$r->{$date}{cases} >= 10) {
		
		$f->{startDate} = $date;
	    }
	}
    }
}



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

sub printCsvNarrow {
    my $data = shift;
    my $dates = shift;
    my $dir = shift;
    my $filePrefix = shift;
    my $statColPairs = shift; #ordered array of pairs of stat name and logical col name
    
    my @stats = map {$_->[0]} @$statColPairs;
    my @cols = map {$_->[1]} @$statColPairs;
    
    my $filepath = "$dir/${filePrefix}.csv";
    
    open FILE, ">$filepath" or die "Failed to open '$filepath' for writing.\n$!\n";
    
    print FILE "key,nation,state,county,date";
    for my $col (@cols) {
	print FILE ",$col";
    }
    print FILE "\n";
    
    
    for my $locid (sort keys %$data) {
	my $key = join('-', (split ',', $locid));
	
	for my $date (@$dates) {
	    my $r = $data->{$locid}{$date};
	    
	    next if (exists($r->{_fake}));
	    
	    print FILE "$key,$locid,$date";
	    for my $stat (@stats) {
		print FILE ",$r->{$stat}";
	    }
	    print FILE "\n";
	}
    }
    
    close FILE;     
}

sub printCsvFocus {
    my $data = shift;
    my $dates = shift;
    my $dir = shift;
    my $filePrefix = shift;
    my $stat = shift;
    my $f = shift; #focus record
    
    my $filepath = "$dir/${filePrefix}-$f->{name}.csv";

    open FILE, ">$filepath" or die "Failed to open '$filepath' for writing\n$!\n";
    
    print FILE "date";
    for my $locid (@{$f->{locids}}) {
	my $formattedLOCID = $locid;
	$formattedLOCID =~ s/\,/-/g;
	$formattedLOCID =~ s/^-//;
	$formattedLOCID =~ s/-$//;
	print FILE ",$formattedLOCID";
    }
    print FILE "\n";

    for my $date (@$dates) {
	next if($date le $f->{startDate});
	
	print FILE "$date";
	for my $locid (@{$f->{locids}}) {
	    print FILE ",$data{$locid}{$date}{$stat}";	
	}
	print FILE "\n";
    }
    close FILE;    
}

printCsvNarrow(\%data,\@dates,$dir,'All',
	       [
		['cases','Cases'],
		['deltaCases','Delta'],
		['deltaNormalCases','DeltaNormal'],
		['batchDeltaCases',"${batchNumDays}DayDelta"],
		['batchDeltaNormalCases',"${batchNumDays}DayDeltaNormal"],
		['doubleSpeed','DoubleSpeed']
	       ]
    );

printCsvData(\%data,\@dates,     $dir,'Total',                        'cases',          'cases');
printCsvData(\%data,\@dates,     $dir,'Delta',                        'deltaCases',     'cases');
printCsvData(\%data,\@dates,     $dir,'DeltaNormal',                        'deltaNormalCases',     'cases');
printCsvData(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",      'batchDeltaCases','cases');
printCsvData(\%data,\@batchDates,$dir,"${batchNumDays}DayDeltaNormal",'batchDeltaNormalCases','cases');
printCsvData(\%data,\@dates,     $dir,'DoubleSpeed',                  'doubleSpeed',    'cases');

for my $f (@focuses) {
    printCsvFocus(\%data,\@dates,     $dir,'Total',                  'cases',          $f);
    printCsvFocus(\%data,\@dates,     $dir,'Delta',                  'deltaCases',     $f);
    printCsvFocus(\%data,\@dates,     $dir,'DeltaNormal',                  'deltaNormalCases', $f);
    printCsvFocus(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",'batchDeltaCases',$f);
    printCsvFocus(\%data,\@batchDates,$dir,"${batchNumDays}DayDeltaNormal",'batchDeltaNormalCases', $f);
    printCsvFocus(\%data,\@dates,     $dir,'DoubleSpeed',            'doubleSpeed',    $f);
}

if ($gitCommit) {
    my $time = `date`;
    chomp $time;

    my $cmd = '';
    
    $cmd = "git commit -a -m 'Updated data at $time'";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";
    
    $cmd = "git push -u origin master";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";    
}
