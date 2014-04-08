
use strict;
use warnings;

use 5.010000;

use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Yet Another RESTful-Archive Service Client",
  "AUTHOR" => "Graham Ollis <plicease\@cpan.org>",
  "BUILD_REQUIRES" => {},
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
    "Clustericious::Client" => "0.82",
    "Clustericious::Client::Command" => 0,
    "Clustericious::Config" => "0.25",
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
    "Mojolicious" => "4.00",
    "Number::Bytes::Human" => "0.09",
    "lib" => 0
  },
  "TEST_REQUIRES" => {
    "Cwd" => 0,
    "File::Temp" => 0,
    "Test::More" => 0
  },
  "VERSION" => "0.94",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


my %FallbackPrereqs = (
  "Clustericious::Client" => "0.82",
  "Clustericious::Client::Command" => 0,
  "Clustericious::Config" => "0.25",
  "Clustericious::Log" => 0,
  "Cwd" => 0,
  "Data::Dumper" => 0,
  "Digest::file" => 0,
  "File::Basename" => 0,
  "File::Spec" => 0,
  "File::Temp" => 0,
  "IO::Uncompress::Gunzip" => 0,
  "Log::Log4perl" => 0,
  "Log::Log4perl::CommandLine" => 0,
  "Mojo::Asset::File" => 0,
  "Mojo::Base" => 0,
  "Mojo::ByteStream" => 0,
  "Mojo::URL" => 0,
  "Mojolicious" => "4.00",
  "Number::Bytes::Human" => "0.09",
  "Test::More" => 0,
  "lib" => 0
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.63_03) } ) {
  delete $WriteMakefileArgs{TEST_REQUIRES};
  delete $WriteMakefileArgs{BUILD_REQUIRES};
  $WriteMakefileArgs{PREREQ_PM} = \%FallbackPrereqs;
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);



