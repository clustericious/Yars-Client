#!perl

use Test::More;
use Yars::Client;
use Log::Log4perl;
use Mojo::ByteStream 'b';
use Mojo::Asset::File;
use File::Temp;
use File::Path qw/remove_tree/;
use FindBin qw/$Bin/;

use strict;

# Round trip upload, download, and removal of a very small file


Log::Log4perl->easy_init( level => "TRACE" );

diag "Contacting Yars";

my $r = Yars::Client->new;

my $server = $r->server_url;

if ( $server =~ /ops/i ) {
    BAIL_OUT "Server contains /ops/.  Not running put tests";
}

my $tx;
ok $r->upload( $Bin . '/../data/welcome' ), "uploaded a file";
ok $r->res->is_status_class(200), "status is 2xx";

is $r->res->headers->location, $r->location("welcome","0bb3c30dc72e63881db5005f1aa19ac3"), "right location";

ok $r->upload( $Bin . '/../data/welcome' ), "uploaded same file";
ok $r->res->is_status_class(200), "Yars (201)";

like $r->get('welcome', '0bb3c30dc72e63881db5005f1aa19ac3'), qr/^welcome/, 'get file';
diag $r->errorstring;
ok !$r->get('Fred','yabbadabba');
is $r->res->code, 404, 'get bogus file';

my $temp_dir = File::Temp->newdir;
ok $r->download( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3', $temp_dir ), "download";
is $r->res->code, 200, "200 status code";

my $asset   = Mojo::Asset::File->new( path => "$temp_dir/welcome" );
my $content = $asset->slurp;
my $md5     = b($content)->md5_sum->to_string;

ok( $md5 eq '0bb3c30dc72e63881db5005f1aa19ac3', 'content' );

ok $r->check( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3'), 'check worked';
ok !$r->check( 'welcome', '1bb3c30dc72e63881db5005f1aa19ac3'), 'check worked';

ok $r->remove( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3'), "called remove";
is ($r->res->code, 200, 'status was 200');

{
my $content = "hello there this is some content";
my $md5 = b($content)->md5_sum;
ok $r->put("myfile.txt", $content);
ok $r->check("myfile.txt",$md5), "checked content";
is $r->get($md5,"myfile.txt"), $content, "got content back";
ok $r->remove("myfile.txt", $md5), "removed $md5";
diag $r->res->error;
}

done_testing();

1;
