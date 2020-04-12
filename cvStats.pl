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

my $reportedFile = "$dir/reported.csv";
my $deltasFile = "$dir/deltas.csv";
my $batchDeltasFile = "$dir/batch.csv";
my $narrowBatchDeltasFile = "$dir/batch-narrow.csv";
my $speedFile = "$dir/speed.csv";

my $data = get $URL or die "Failed to open '$URL'\n$0";

#REMOVE ME:
my $fakeData = 
"date,county,state,fips,cases,deaths
03-01-2020,NYC,NY,123,0,0
03-02-2020,NYC,NY,123,1,0
03-03-2020,NYC,NY,123,2,0
03-04-2020,NYC,NY,123,3,0
03-05-2020,NYC,NY,123,4,0
03-06-2020,NYC,NY,123,6,0
03-07-2020,NYC,NY,123,8,0
03-08-2020,NYC,NY,123,12,0
03-09-2020,NYC,NY,123,16,0
03-10-2020,NYC,NY,123,24,0
03-11-2020,NYC,NY,123,32,0
03-12-2020,NYC,NY,123,48,0
03-13-2020,NYC,NY,123,64,0
03-14-2020,NYC,NY,123,96,0
03-15-2020,NYC,NY,123,128,0
";

#$data = $fakeData; #REMOVE ME

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
my $batchNumDays = 3;
my @batchFilter = ('New York,New York City','New York,Nassau','Arizona,Maricopa');
my %batchFilter = map {$_ => 1} @batchFilter;

sub log2 {
    return log($_[0])/log(2);
}

#enrich with deltas and fill in blank dates
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


open REPORTEDFILE, ">$reportedFile" or die "Failed to open $reportedFile for writing\n$!\n";

print REPORTEDFILE "state,county";
for my $date (@dates) {
    my $monthDay = $date; $monthDay =~ s/^202.-//;    
    print REPORTEDFILE ",$monthDay";
}
print REPORTEDFILE "\n";

for my $cid (sort {$data{$b}{$dates[-1]}{cases} <=> $data{$a}{$dates[-1]}{cases}} keys %data) {
    print REPORTEDFILE "$cid";
    for my $date (@dates) {
	print REPORTEDFILE ",$data{$cid}{$date}{cases}";
    }
    print REPORTEDFILE "\n";
}

close REPORTEDFILE;





open DELTASFILE, ">$deltasFile" or die "Failed to open $deltasFile for writing\n$!\n";

print DELTASFILE "state,county";
for my $date (@dates) {
    my $monthDay = $date; $monthDay =~ s/^202.-//;    
    print DELTASFILE ",$monthDay";
}
print DELTASFILE "\n";

for my $cid (sort {$data{$b}{$dates[-1]}{cases} <=> $data{$a}{$dates[-1]}{cases}} keys %data) {
    print DELTASFILE "$cid";
    for my $date (@dates) {
	print DELTASFILE ",$data{$cid}{$date}{deltaCases}";
    }
    print DELTASFILE "\n";
}

close DELTASFILE;



open BATCHFILE, ">$batchDeltasFile" or die "Failed to open $batchDeltasFile for writing\n$!\n";

print BATCHFILE "state,county";
for my $date (@batchDates) {
    my $monthDay = $date; $monthDay =~ s/^202.-//;    
    print BATCHFILE ",$monthDay";    
}
print BATCHFILE "\n";

for my $cid (sort {$data{$b}{$dates[-1]}{cases} <=> $data{$a}{$dates[-1]}{cases}} keys %data) {
    print BATCHFILE "$cid";
    for my $date (@batchDates) {
	print BATCHFILE ",$data{$cid}{$date}{batchDeltaCases}";
    }
    print BATCHFILE "\n";
}
close BATCHFILE;

#print Dumper(\%data);

open NARROWBATCHFILE, ">$narrowBatchDeltasFile" or die "Failed to open $narrowBatchDeltasFile for writing\n$!\n";

print NARROWBATCHFILE "date";
for my $cid (@batchFilter) {
    my $formattedCID = $cid;
    $formattedCID =~ s/\,/-/;
    print NARROWBATCHFILE ",$formattedCID";
}
print NARROWBATCHFILE "\n";

for my $date (@batchDates) {
    print NARROWBATCHFILE "$date";
    for my $cid (@batchFilter) {
	print NARROWBATCHFILE ",$data{$cid}{$date}{batchDeltaCases}";	
    }
    print NARROWBATCHFILE "\n";
}
close NARROWBATCHFILE;







open SPEEDFILE, ">$speedFile" or die "Failed to open $speedFile for writing\n$!\n";

print SPEEDFILE "state,county";
for my $date (@dates) {
    my $monthDay = $date; $monthDay =~ s/^202.-//;    
    print SPEEDFILE ",$monthDay";    
}
print SPEEDFILE "\n";

for my $cid (sort {$data{$b}{$dates[-1]}{cases} <=> $data{$a}{$dates[-1]}{cases}} keys %data) {
    print SPEEDFILE "$cid";
    for my $date (@dates) {
	print SPEEDFILE ",$data{$cid}{$date}{doubleSpeed}";
    }
    print SPEEDFILE "\n";
}

close SPEEDFILE;


if ($gitCommit) {
    my $time = `date`;
    chomp $time;

    my $cmd = '';
    
    $cmd = "git commit -a -m 'Updated data at $time'";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";
    
    $cmd = "git push -u origin master";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";    
}
