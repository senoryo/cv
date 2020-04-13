#!/usr/bin/perl
use strict;

use LWP::Simple;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

my $URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv';

#date,county,state,fips,cases,deaths
#2020-01-21,Snohomish,Washington,53061,1,0

my $USAGE = "Usage: $0 [--git-commit] <output-dir>";

my $gitCommit = 0;
if ($ARGV[0] =~ m/^-/) {
    $ARGV[0] eq '--git-commit' or die "Invalid option '$ARGV[0]'\n$USAGE\n";
    shift @ARGV;
    $gitCommit = 1; 
}

my $dir = shift @ARGV or die "$USAGE\n";


my $data = get $URL or die "Failed to open '$URL'\n$0";


open DATA, "<", \$data;

my $header = <DATA>;
chomp $header;
my @header = split ',', $header;
my $headerExp = 'date,county,state,fips,cases,deaths';
$header eq $headerExp or die "Expected header '$headerExp'\nGot '$header'\n";

my %data = ();
my %dates = ();

my $dateCount = 0;
while (my $line = <DATA>) {
    chomp $line;
    my ($date,$county,$state,$fips,$cases,$deaths) = split ',', $line;
    #next unless $county eq 'New York City'; #REMOVE ME

    my $countyID = "$state,$county"; #unique countID (but also convenient "state,county"
    $data{$countyID}{$date} = {cases=>$cases, deaths=>$deaths, _date=>$date};
    
    if (!exists($dates{$date})) {
	$dates{$date} = ++$dateCount;
    }
}
close DATA;

my @dates = sort keys %dates;


my %batchDates = ();
my $batchNumDays = 7;

my @focusAreas = ('New York,New York City','New York,Nassau');
my %focusAreas = map {$_ => 1} @focusAreas;
my $focusAreasStartDate = undef;

sub log2 {
    return log($_[0])/log(2);
}

#enrich with deltas, fill in blank dates, set focus areas start date
while (my ($cid,$r) = each %data) {
    my $prev = undef;
    my $batchDay = 0;
    my $batchHead = undef;
    
    for my $date (sort {$b cmp $a} @dates) { #reverse order
	my $curr = undef;
	if (!exists($r->{$date})) {
	    $r->{$date} = {_date=>$date,cases=>0,deaths=>0,deltaCases=>0,deltaDeaths=>0};
	}
	
	$curr = $r->{$date};

	if ($curr->{cases} > 0) {
	    $curr->{logCases} = log2($curr->{cases});
	}

	if (exists($focusAreas{$cid}) &&
	    !defined($focusAreasStartDate) &&
	    $curr->{cases} > 10) { 
	    $focusAreasStartDate = $date;
	}
	
	
	#print "CURR [$date]: ";print Dumper($curr);
	
	## 1-Day delta ##
	if (defined $prev) {
	    $prev->{deltaCases} = $prev->{cases} - $curr->{cases};
	    $prev->{deltaDeaths} = $prev->{deaths} - $curr->{deaths};
	    if (exists($curr->{logCases}) && exists($prev->{logCases})) {
		$prev->{deltaLogCases} = $prev->{logCases} - $curr->{logCases};
	    }
	}
	$prev = $curr;
	
	#print "  PREV [$date]: ";print Dumper($prev);
	
	## N-Days Delta (aka Batch Delta) ##
	++$batchDay;
	$batchDay = 1 if($batchDay > $batchNumDays);
	
	if ($batchDay == 1) { #UNCOMMENT ME
#	if ($batchDay == 0) { #REMOVE ME
	    if (defined $batchHead) {
		$batchHead->{batchDeltaCases} = $batchHead->{cases} - $curr->{cases};
		$batchHead->{batchDeltaDeaths} = $batchHead->{deaths} - $curr->{deaths};
	    }
	    $batchHead = $curr;
	    $batchDates{$date} = 1;
	}
    }
}

my @batchDates = sort keys %batchDates;

#calc double speed
while (my ($cid,$r) = each %data) {
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
    
    print FILE "state,county";
    for my $date (@$dates) {
	print FILE ",$date";
    }
    print FILE "\n";
    
    
    for my $cid (sort {$data->{$b}{$dates->[-1]}{cases} <=> $data->{$a}{$dates->[-1]}{cases}} keys %$data) {
	print FILE "$cid";
	for my $date (@$dates) {
	    print FILE ",$data->{$cid}{$date}{$stat}";
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
    for my $cid (@$focusAreas) {
	my $formattedCID = $cid;
	$formattedCID =~ s/\,/-/;
	print FILE ",$formattedCID";
    }
    print FILE "\n";
    
    for my $date (@$dates) {
	next if($date le $focusAreasStartDate);
	
	print FILE "$date";
	for my $cid (@$focusAreas) {
	    print FILE ",$data{$cid}{$date}{$stat}";	
	}
	print FILE "\n";
    }
    close FILE;    
}

printCsvData(\%data,\@dates,     $dir,'Total',                  'cases',          'cases');
printCsvData(\%data,\@dates,     $dir,'Delta',                  'deltaCases',     'cases');
printCsvData(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",'batchDeltaCases','cases');
printCsvData(\%data,\@dates,     $dir,'DoubleSpeed',            'doubleSpeed',    'cases');

printCsvFocusAreas(\%data,\@dates,     $dir,'Total',                  'cases',          \@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@dates,     $dir,'Delta',                  'deltaCases',     \@focusAreas,$focusAreasStartDate);
printCsvFocusAreas(\%data,\@batchDates,$dir,"${batchNumDays}DayDelta",'batchDeltaCases',\@focusAreas,$focusAreasStartDate);
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
