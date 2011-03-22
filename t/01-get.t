#!perl

use Test::More $ENV{YC_LIVE_TESTS} ? "no_plan" : (skip_all => "Set RC_LIVE_TESTS to use restmd configuration ");
use Yars::Client;
use Log::Log4perl;

use strict;

Log::Log4perl->easy_init(level => "WARN");

diag "Contacting Yars server";

my $yc = Yars::Client->new;

ok $yc, "made a client object";

my $welcome = $yc->welcome;

like $welcome, qr/welcome to yars/i, "got welcome message";

1;

