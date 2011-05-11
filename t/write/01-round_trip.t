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

like( $r->upload( $Bin . '/../data/welcome' ), qr/^201/, 'upload' );

like( $r->content('welcome', '0bb3c30dc72e63881db5005f1aa19ac3'), qr/^welcome/, 'content' );


my $temp_dir = tempdir( );
like( $r->download( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3', $temp_dir ),
    qr/^200/, 'download' );


my $asset   = Mojo::Asset::File->new( path => "$temp_dir/welcome" );
my $content = $asset->slurp;
my $md5     = b($content)->md5_sum->to_string;

ok( $md5 eq '0bb3c30dc72e63881db5005f1aa19ac3', 'content' );

like( $r->remove( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3' ),
    qr/^200/, 'remove' );

remove_tree($temp_dir);

done_testing();

1;
