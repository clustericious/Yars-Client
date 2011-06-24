#!perl

use Test::More;
use Yars::Client;
use Log::Log4perl;
use Mojo::ByteStream 'b';
use Mojo::Asset::File;
use File::Temp qw/tempdir/;
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
ok( $tx = $r->upload( $Bin . '/../data/welcome' ), "uploaded a file" );
ok($tx->success, "tx was successful");
is $tx->res->code, 201, "status is 201 (created)";

like( $r->retrieve('welcome', 
    '0bb3c30dc72e63881db5005f1aa19ac3')->res->body, qr/^welcome/, 'retrieve' );
$tx = $r->retrieve('Fred','yabbadabba'); 
ok($tx->res->code == 404, 'retrieve - not found');


my $temp_dir = tempdir( );
ok( $tx = $r->download( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3', $temp_dir ), "download");
ok( $tx->success, "download successful" );
is $tx->res->code, 200, "200 status code";

my $asset   = Mojo::Asset::File->new( path => "$temp_dir/welcome" );
my $content = $asset->slurp;
my $md5     = b($content)->md5_sum->to_string;

ok( $md5 eq '0bb3c30dc72e63881db5005f1aa19ac3', 'content' );

ok( $tx = $r->remove( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3'), "called remove");
ok($tx->success, "remove was successful");
is ($tx->res->code, 200, 'status was 200');

remove_tree($temp_dir);

done_testing();

1;
