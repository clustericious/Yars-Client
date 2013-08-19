#!perl

# 001_buckets.t

use Test::More;
use Yars::Client;
use Data::Dumper;
use File::Temp;
use File::Basename qw/basename/;
use Mojo::ByteStream qw/b/;
use Mojo::UserAgent;

use strict;
use warnings;

my $c = Yars::Client->new(
    server_url  => "http://localhost:3002",
);
Yars::Client->get_logger()->level("WARN");
Clustericious::Client->get_logger()->level("WARN");

my $status = $c->status;

unless ($status) {
    diag "To run these tests, start a test Yars server like this : ";
    diag "./Build test --test-files=t/integrate/001_start.t";
}

like $status->{server_version}, qr/\d/, "server version is numeric";

my $map = $c->bucket_map;
ok defined($map);

my $stats = $c->disk_usage;
ok defined($stats);

srand 1;

for (0..20) {
    my $filename;
    my $location;
    my $content = "some data $$".rand;
    my $md5 = b($content)->md5_sum;
    {
        my $t = File::Temp->new();
        $filename = basename "$t";
        print $t $content;
        $t->close;
        ok $c->upload("$t");
        $location = $c->res->headers->location;
    }
    ok ! -e $filename, "temp file was cleaned up";

    ok defined($location), "got a location";
    my $tempdir = File::Temp->newdir;
    my $content_back = $c->get($filename,$md5);
    ok $content_back, 'get succeeded';
    is $content_back, $content, "got content back";
    ok $c->download($filename,$md5,"$tempdir"), "download succeeded";
    diag $c->errorstring if $c->errorstring;

    ok -e "$tempdir/$filename", "Wrote to $tempdir/$filename";
    my $got = -e "$tempdir/$filename" ? b(join '', IO::File->new("$tempdir/$filename")->getlines)->md5_sum : '';
    is $got, $md5, "got right md5 back";
}

my $up = $c->servers_status;
for my $server (keys %$up) {
    for my $disk (keys %{ $up->{$server} } ) {
        is $up->{$server}{$disk}, 'up', "Server $server, disk $disk is up";
    }
}

done_testing();

1;

