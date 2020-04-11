#!/usr/bin/perl
use strict;

use LWP::Simple;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

my $URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv';

#date,county,state,fips,cases,deaths
#2020-01-21,Snohomish,Washington,53061,1,0

my $USAGE = "Usage: $0 [--git-commit] <reported-output-file> <deltas-output-file> <batch-deltas-output-file> <speed-output-file>";

my $gitCommit = 0;
if ($ARGV[0] =~ m/^-/) {
    $ARGV[0] eq '--git-commit' or die "Invalid option '$ARGV[0]'\n$USAGE\n";
    shift @ARGV;
    $gitCommit = 1; 
}


my $reportedFile = shift @ARGV or die "$USAGE\n";
my $deltasFile = shift @ARGV or die "$USAGE\n";
my $batchDeltasFile = shift @ARGV or die "$USAGE\n";
my $speedFile = shift @ARGV or die "$USAGE\n";


my $data = get $URL or die "Failed to open '$URL'\n$0";

open DATA, "<", \$data;

my $header = <DATA>;
chomp $header;
my @header = split ',', $header;
my $headerExp = 'date,county,state,fips,cases,deaths';
$header eq $headerExp or die "Expected header '$headerExp'\nGot '$header'\n";

my %data = ();
my %dates = ();

while (my $line = <DATA>) {
    chomp $line;
    my ($date,$county,$state,$fips,$cases,$deaths) = split ',', $line;
    #next unless $county eq 'New York City'; #REMOVE ME

    my $countyID = "$state,$county"; #unique countID (but also convenient "state,county"
    $data{$countyID}{$date} = {cases=>$cases, deaths=>$deaths, _date=>$date};
    $dates{$date} = 1;
}
close DATA;

my @dates = sort keys %dates;


my %batchDates = ();
my $batchNumDays = 3;

#enrich with deltas and fill in blank dates
while (my ($cid,$r) = each %data) {
    my $prev = undef;
    my $batchDay = 0;
    my $batchHead = undef;
    
    for my $date (sort {$b cmp $a} @dates) { #reverse order
	my $curr = undef;
	if (!exists($r->{$date})) {
	    $r->{$date} = {cases=>0,deaths=>0,deltaCases=>0,deltaDeaths=>0};
	}
	
	$curr = $r->{$date};

	#print "CURR [$date]: ";print Dumper($curr);
	
	## 1-Day delta ##
	if (defined $prev) {
	    $prev->{deltaCases} = $prev->{cases} - $curr->{cases};
	    $prev->{deltaDeaths} = $prev->{deaths} - $curr->{deaths};
	}
	$prev = $curr;
	
	#print "  PREV [$date]: ";print Dumper($prev);
	
	## N-Days Delta (aka Batch Delta) ##
	++$batchDay;
	$batchDay = 1 if($batchDay > $batchNumDays);
	
	if ($batchDay == 1) {
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

#print Dumper(\%data);


#calculate "double speed" -- i.e. # of days it takes to double
my %doubleSpeed = ();

while (my ($cid,$r) = each %data) {
    my $benchDate = '';
    my $prevDate = '';
    
    my $days = undef;
    for my $date (sort {$b cmp $a} @dates) { #iterate in reverse
	if ($benchDate eq '') {
	    $benchDate = $date;
	    $prevDate = $date;
	    $days = 0;
	}
	else {
	    my $benchCases = $r->{$benchDate}{cases};
	    my $prevCases = $r->{$prevDate}{cases};
	    my $cases = $r->{$date}{cases};
	    
	    my $halfBench = $benchCases / 2;
	    
	    #print "\n$state - $county\n";
	    #print "  date=$date\n";
	    #print "  benchDate=$benchDate\n";
	    #print "  prevDate=$prevDate\n";
	    #print "  benchCases=$benchCases\n";
	    #print "  prevCases=$prevCases\n";
	    #print "  cases=$cases\n";
	    #print "  halfBench=$halfBench\n";
	    #print "  days=$days\n";
	    
	    if ($benchCases == 0) {
		last;
	    }
	    
	    if ($cases <= $halfBench) { #i.e. bench > 2x cases
		my $benchToPrev = $prevCases - $halfBench;
		my $casesToPrev = $prevCases - $cases;
		my $fraction = $benchToPrev / $casesToPrev;
		
		$doubleSpeed{$cid}{doubleSpeed} = $days + $fraction;
		$doubleSpeed{$cid}{latestCases} = $benchCases; 
		
		last;
	    }
	    
	    ++$days;
	    $prevDate = $date;
	}
    }
}    

#print "\nDouble speed:\n";
#print Dumper(\%doubleSpeed);

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









open SPEEDFILE, ">$speedFile" or die "Failed to open $speedFile for writing\n$!\n";

print SPEEDFILE "state,county,latestCases,doubleSpeed\n";

for my $cid (sort {$doubleSpeed{$a}{doubleSpeed} <=> $doubleSpeed{$b}{doubleSpeed}} keys %doubleSpeed) {
    if ($doubleSpeed{$cid}{latestCases} >= 2000) {
	printf SPEEDFILE "$cid,$doubleSpeed{$cid}{latestCases},%.1f\n", $doubleSpeed{$cid}{doubleSpeed};
    }
}

close SPEEDFILE;


if ($gitCommit) {
    my $time = `date`;
    chomp $time;

    my $cmd = '';
    
    $cmd = "git commit -m 'Updated data at $time'";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";
    
    $cmd = "git push -u origin master";
    system $cmd and die "Failed to execute '$cmd'\n$!\n";    
}
