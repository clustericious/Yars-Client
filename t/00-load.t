#!perl

use Test::More tests => 1;

BEGIN {
    use_ok( 'Yars::Client' );
}

diag( "Testing Yars::Client, Perl $], $^X" );
