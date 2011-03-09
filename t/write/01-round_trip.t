#!perl

use Test::More tests => 4;
use RESTAS::Yarc;
use Log::Log4perl;
use Mojo::ByteStream 'b';
use Mojo::Asset::File;
use File::Temp qw/tempdir/;
use FindBin qw/$Bin/;
use Cwd;

use strict;

Log::Log4perl->easy_init( level => "WARN" );

diag "Contacting RESTAS server";

my $r = RESTAS::Yarc->new;

my $server = $r->server_url;

if ( $server =~ /ops/i ) {
    BAIL_OUT "Server contains /ops/.  Not running put tests";
}

like( $r->upload( $Bin . '/../data/welcome' ), qr/uploaded/, 'upload' );

my $saved_dir = getcwd;
my $temp_dir = tempdir( CLEANUP => 1 );
chdir $temp_dir;

like( $r->download( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3' ),
    qr/downloaded/, 'download' );

my $asset   = Mojo::Asset::File->new( path => $temp_dir . '/welcome' );
my $content = $asset->slurp;
my $md5     = b($content)->md5_sum->to_string;
ok( $md5 eq '0bb3c30dc72e63881db5005f1aa19ac3', 'content' );

chdir $saved_dir;

like( $r->remove( 'welcome', '0bb3c30dc72e63881db5005f1aa19ac3' ),
    qr/deleted/, 'remove' );

1;
