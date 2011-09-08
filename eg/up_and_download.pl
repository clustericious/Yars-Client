#!/usr/bin/env perl

use Yars::Client;
use strict;

mkdir 'files';
for (1..100) {
    open my $fp, ">files/file.$_";
    print $fp "some data $_";
    print $fp 'more data' for 1..100;
    close $fp;
}

print "uploading\n";
my $y = Yars::Client->new();
my @locations;
for (1..100) {
    my $got = $y->upload("files/file.$_");
    if (my $res = $got->success) {
        print "ok : ".$res->code."\n";
        #print "ok : ".$res->to_string."\n"; for more verbose info
        push @locations, $res->headers->location;
    } else {
        print "not ok : ".$got->error;
    }
}


system ('rm -rf ./got');
mkdir 'got';
chdir 'got';

for (1..100) {
    $y->download(shift @locations) or print "fail :$_".$y->errorstring;
}

chdir '..';

system 'diff -r files/ got/';


