#!/usr/bin/env perl

use Yars::Client;
use strict;

my $how_many = 100;

mkdir 'files';
for (1..$how_many) {
    open my $fp, ">files/file.$_";
    print $fp "some data $_";
    print $fp 'more data' for 1..$how_many;
    close $fp;
}

print "uploading\n";
my $y = Yars::Client->new();
my @locations;
for (1..$how_many) {
    $y->upload("files/file.$_") or print $y->errorstring;
    push @locations, $y->res->headers->location;
}


system ('rm -rf ./got');
mkdir 'got';
chdir 'got';

for (1..$how_many) {
    my $loc = shift @locations;
    $y->download($loc) or print "failed to get $loc: ".$y->errorstring."\n";
}

chdir '..';

system 'diff -r files/ got/';


