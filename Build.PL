use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'RESTAS::Yarc',
    license             => 'perl',
    dist_author         => q{Marty Brandon<mbrandon@sesda2.com>},
    dist_version_from   => 'lib/RESTAS/Yarc.pm',
    dist_abstract       => 'Yet Another RESTAS Client',
    requires => {
        'Clustericious::Client' => '0.07',
        'Clustericious::Config' => '0.04',
    },
    build_requires => {
        'Test::More' => 0,
    },
    add_to_cleanup      => [ 'RESTAS-Yarc-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
