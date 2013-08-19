#!perl

use Test::More $ENV{YC_LIVE_TESTS} ? "no_plan" : (skip_all => "Set YC_LIVE_TESTS to use Yars configuration ");
use Yars::Client;
use Log::Log4perl;
use File::Temp;
use Cwd qw/getcwd/;

use strict;
use warnings;

Log::Log4perl->easy_init(level => "WARN");

diag "Contacting Yars server";

my $yc = Yars::Client->new;

ok $yc, "made a client object";

my $welcome = $yc->welcome;

like $welcome, qr/welcome to yars/i, "got welcome message";

my $status = $yc->status;
ok $status->{server_version}, 'server status';

1;

