use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'Yars::Client',
    license             => 'perl',
    dist_author         => q{Marty Brandon<mbrandon@sesda2.com>},
    dist_version_from   => 'lib/Yars/Client.pm',
    dist_abstract       => 'Yars Client',
    requires => {
        'Clustericious::Client' => '0.29',
        'Clustericious::Config' => '0.04',
        'JSON'                  => '0',
    },
    build_requires => {
        'Test::More' => 0,
    },
    add_to_cleanup      => [ 'Yars-Client-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
