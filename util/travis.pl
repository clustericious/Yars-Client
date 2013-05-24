use strict;
use warnings;
use YAML qw( DumpFile );
use File::HomeDir;
use Mojo::IOLoop::Server;

my $port = Mojo::IOLoop::Server->generate_port;

mkdir(File::HomeDir->my_home . '/etc');
mkdir(File::HomeDir->my_home . '/data');
DumpFile(File::HomeDir->my_home . '/etc/Yars.conf' => {
  url => "http://localhost:$port",
  start_mode => 'hypnotoad',
  hypnotoad => {
    listen => "http://localhost:$port",
    pid_file => File::HomeDir->my_home . "/yars.pid",
  },
  servers => [ {
    url => "http://localhost:$port",
    disks => [ {
      root => File::HomeDir->my_home . '/data',
      buckets => [ 0..9, 'a'..'f' ],
    } ],
  } ],
});
