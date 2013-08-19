
use strict;
use warnings;

use 5.010000;

use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Yet Another RESTful-Archive Service Client",
  "AUTHOR" => "Graham Ollis <plicease\@cpan.org>",
  "BUILD_REQUIRES" => {
    "Cwd" => 0,
    "File::Temp" => 0,
    "Test::More" => 0
  },
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.30"
  },
  "DISTNAME" => "Yars-Client",
  "EXE_FILES" => [
    "bin/yarsclient"
  ],
  "LICENSE" => "perl",
  "NAME" => "Yars::Client",
  "PREREQ_PM" => {
    "Clustericious::Client" => "0.54",
    "Clustericious::Client::Command" => 0,
    "Clustericious::Config" => "0.04",
    "Clustericious::Log" => 0,
    "Data::Dumper" => 0,
    "Digest::file" => 0,
    "File::Basename" => 0,
    "File::Spec" => 0,
    "IO::Uncompress::Gunzip" => 0,
    "Log::Log4perl" => 0,
    "Log::Log4perl::CommandLine" => 0,
    "Mojo::Asset::File" => 0,
    "Mojo::Base" => 0,
    "Mojo::ByteStream" => 0,
    "Mojo::URL" => 0,
    "Number::Bytes::Human" => "0.09"
  },
  "VERSION" => "0.89",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.56) } ) {
  my $br = delete $WriteMakefileArgs{BUILD_REQUIRES};
  my $pp = $WriteMakefileArgs{PREREQ_PM};
  for my $mod ( keys %$br ) {
    if ( exists $pp->{$mod} ) {
      $pp->{$mod} = $br->{$mod} if $br->{$mod} > $pp->{$mod};
    }
    else {
      $pp->{$mod} = $br->{$mod};
    }
  }
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);



