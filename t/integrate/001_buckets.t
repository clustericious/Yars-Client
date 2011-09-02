#!perl

# 001_buckets.t

use Yars::Client;
use Test::More;
use Data::Dumper;
use File::Temp;
use File::Basename qw/basename/;
use Mojo::ByteStream qw/b/;
use Mojo::UserAgent;

use strict;
use warnings;

my $c = Yars::Client->new(
    server_url  => "http://localhost:9051",
    server_type => "Yars"
);
#$c->client->log->level('FATAL');

my $status = $c->status;

unless ($status) {
    diag "To run these tests, start a test Yars server like this : ";
    diag "./Build test --test-files=t/integrate/001_start.t";
}

like $status->{server_version}, qr/\d/, "server version is numeric";

my $map = $c->bucket_map;
ok defined($map);

my $stats = $c->disk_stats;
ok defined($stats);

my $filename;
my $location;
my $content = "some data $$".rand;
my $md5 = b($content)->md5_sum;
{
    my $t = File::Temp->new();
    $filename = basename "$t";
    print $t $content;
    $t->close;
    my $tx = $c->upload("$t");
    my $res = $tx->success;
    $location = $res->headers->location;
}
ok ! -e $filename, "temp file was cleaned up";

ok defined($location), "got a location";
my $tempdir = File::Temp->newdir;
my $tx = $c->download($filename,$md5,"$tempdir");
my $res;
ok $res = $tx->success, "Download was a success";

my $got = b(join '', IO::File->new("$tempdir/$filename")->getlines)->md5_sum;
is $got, $md5, "got right md5 back";

done_testing();

1;

