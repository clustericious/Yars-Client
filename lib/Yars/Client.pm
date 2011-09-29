package Yars::Client;

use strict;
use warnings;

use Clustericious::Client;
use Clustericious::Client::Command;
use Clustericious::Config;
use Mojo::Asset::File;
use Mojo::ByteStream 'b';
use Mojo::URL;
use Mojo::Base '-base';
use File::Basename;
use File::Spec;
use Log::Log4perl qw(:easy);
use Data::Dumper;
use feature qw(say);

our $VERSION = '0.39';

# default max of 10 GB
$ENV{MOJO_MAX_MESSAGE_SIZE} ||= 1024*1024*1024 * 10;

Clustericious::Client::Meta->add_route( "Yars::Client",
    upload => "<filename> [md5]" );
Clustericious::Client::Meta->add_route( "Yars::Client",
    content => "<filename> <md5>" );
Clustericious::Client::Meta->add_route( "Yars::Client",
    download => "<filename> <md5> [dir]" );
Clustericious::Client::Meta->add_route( "Yars::Client",
    remove => "<filename> <md5>" );

has server_type => sub { shift->_config->server_type(default => 'Yars') };
has bucket_map_cached  => sub { 0; }; # Computed on demand.

route 'welcome'        => "GET",  '/';
route 'bucket_map'     => "GET",  '/bucket_map';
route 'disk_usage'     => "GET",  '/disk/usage';
route 'servers_status' => "GET",  '/servers/status';
route 'get'            => "GET",  '/file', \"<md5> <filename>";
route 'check'          => "HEAD", '/file', \"<md5> <filename>";
route 'set_status'     => "POST", '/disk/status';

sub new {
    my $self = shift->SUPER::new(@_);
    $self->client->max_redirects(30);
    return $self;
}

sub _get_url {

    # Helper to create the Mojo URL objects
    my ($self, $path) = @_;

    my $url = Mojo::URL->new( $self->server_url );
    $url->path($path) if $path;

    return $url;
}

sub location {
    my ($self, $filename, $md5) = @_;

    ( $filename, $md5 ) = ( $md5, $filename ) if $filename =~ /^[0-9a-f]{32}$/i;
    LOGDIE "Can't compute location without filename" unless defined($filename);
    LOGDIE "Can't compute location without md5" unless $md5;
    $self->server_url($self->_server_for($md5));
    return $self->_get_url("/file/$md5/$filename")->to_abs->to_string;
}

sub download {
    # Downloads a file and saves it to disk.
    my $self = shift;
    my ( $filename, $md5, $dest_dir ) = @_;
    my $url;
    if (@_ == 1) {
        $url = shift;
        ($filename) = $url =~ m|/([^/]+)$|;
    }
    ( $filename, $md5 ) = ( $md5, $filename ) if $filename =~ /^[0-9a-f]{32}$/i;

    if (!$md5 && !$url) {
        LOGDIE "Need either an md5 or a url: download(url) or download(filename, md5, [dir] )";
    }
    unless ($url) {
        LOGDIE "Missing md5" unless $md5;
        my $direct = $self->_server_for($md5) or LOGDIE "no server for $md5";
        $self->server_url($direct);
    }

    my $content =                  $url ? $self->_doit( GET => $url )
       : $self->server_type eq 'RESTAS' ? $self->get( $filename, $md5 )
       :                                  $self->get( $md5, $filename );
    return '' if $self->errorstring;
    my $out_file = $dest_dir ? $dest_dir . "/$filename" : $filename;
    DEBUG "Writing to $out_file";
    Mojo::Asset::File->new->add_chunk($content)->move_to($out_file);
    return 'ok';
}

sub remove {
    # Removes a file
    my ( $self, $filename, $md5 ) = @_;

    LOGDIE "file and md5 needed for remove"
        unless $filename && $md5;

    my $url = (
          $self->server_type eq 'RESTAS'
        ? $self->_get_url("/file/$filename/$md5")
        : $self->_get_url("/file/$md5/$filename")
    );
    TRACE("removing $filename $md5 from ", $url->to_string);

    # Delete the file
    $self->_doit(DELETE => $url);
}

# Given an md5, determine the correct server
# using a cached list of bucket->server assignments.
sub _server_for {
    my $self = shift;
    my $md5 = shift or LOGDIE "Missing argument md5";
    return $self->server_url if $self->server_type eq 'RESTAS';
    my $bucket_map = $self->bucket_map_cached;
    unless ($bucket_map) {
        $bucket_map = $self->bucket_map;
        $self->bucket_map_cached($bucket_map);
    }
    for (0..length($md5)) {
        my $prefix = substr($md5,0,$_);
        return $bucket_map->{ lc $prefix } if exists($bucket_map->{lc $prefix});
        return $bucket_map->{ uc $prefix } if exists($bucket_map->{uc $prefix});
    }
    LOGDIE "Can't find url for $md5 in bucket map : ".Dumper($bucket_map);
}

sub put {
    my $self = shift;
    my $filename = shift;
    my $content = shift || join '', <STDIN>;
    my $url = $self->_get_url("/file/$filename");
    my $tx = $self->client->put($url => {} => $content);
    $self->res($tx->res);
    return $tx->success ? 'ok' : '';
}

sub upload {
    my ( $self, $filename ) = @_;

    LOGDIE "file needed for upload" unless $filename;
    $filename = File::Spec->rel2abs($filename);
    -r $filename or LOGDIE "Could not read " . $filename;

    # Read the file
    my $basename = basename($filename);
    my $asset    = Mojo::Asset::File->new( path => $filename );
    my $content  = $asset->slurp;
    my $md5      = b($content)->md5_sum;

    my $url;
    my $tx;
    if ( $self->server_type eq 'RESTAS' ) {

        # Workaround for RESTAS which sends a 409 instead of a 200 when
        # putting a previously putted file.

        $url = $self->_get_url("/file/$basename/$md5");
        my $head_check = $self->client->head($url);
        $tx = $head_check if $head_check->success;
    } else {
        my $assigned = $self->_server_for($md5);
        $url = Mojo::URL->new($assigned);
        $url->path("/file/$basename/$md5");
        DEBUG "Sending $md5 to $url";
    }

    if ( !$tx ) {
        # Either we have a Yars server or the head_check was negative

        $tx = $self->client->put( $url => $content );
        if ( my ($message, $code) = $tx->error ) {
            defined $code ? ERROR "$code $message" : ERROR $message;
            $self->res($tx->res);
            return '';
        }
    }

    DEBUG "Response : ".$tx->res->code." ".$tx->res->message;
    $self->res($tx->res);
    return 'ok';
}

sub status {
    my $self = shift;

    return $self->SUPER::status(@_) unless $self->server_type eq 'RESTAS';

    # Provides a workaround for getting the status of a RESTAS server.

    if ( $self->server_type eq 'RESTAS' ) {
        # RESTAS server status

        # This request never succeeds, but a '404 not found' at least means that
        # the server replied, which we use to indicate that status is ok.
        my $tx = $self->client->head( $self->server_url . '/my_bogus_url' );
        my ($message, $code) = $tx->error;

        my $config = Clustericious::Config->new('Yars');
        my $host = $config->{ssh_tunnel}
            ? $config->{ssh_tunnel}{server_host}
            : $config->{host};

        if (defined $code and $code == 404) {
            my %status = (
                app_name        => 'Yars',
                server_hostname => $host,
                server_url      => $self->server_url,
                server_version  => 'RESTAS',
            );

            $tx->res->error(undef);  # unset the error flag

            return \%status;
        }
        else {
            ERROR $tx->error;
            $self->res($tx->res);
            return;
        }
    }
}

1;

__END__

=head1 NAME

Yars::Client (Yet Another REST Server Client)

=head1 SYNOPSIS

 my $r = Yars::Client->new;

 # Put a file.
 $r->upload($filename) or die $r->errorstring;
 print $r->res->headers->location;

 # Write a file to disk.
 $r->download($filename, $md5) or die $r->errorstring;
 $r->download($filename, $md5, '/tmp');   # download it to the /tmp directory
 $r->download("http://yars/0123456890abc/filename.txt"); # Write filename.txt to current directory.

 # Get the content of a file.
 my $content = $r->get($filename,$md5);

 # Put some content to a filename.
 my $content = $r->put($filename,$content);

 # Delete a file.
 $r->remove($filename, $md5) or die $r->errorstring;

 # Find the URL of a file.
 print $r->location($filename, $md5);

 print "Server version is ".$r->status->{server_version};
 my $usage = $r->disk_usage();      # Returns usage for a single server.
 my $nother_usage = Yars::Client->new(url => "http://anotherserver.nasa.gov:9999")->disk_usage();
 my $status = $r->servers_status(); # return a hash of servers, disks, and their statuses

 # Mark a disk down.
 my $ok = $r->set_status({ root => "/acps/disk/one", state => "down" });
 my $ok = $r->set_status({ root => "/acps/disk/one", state => "down", host => "http://someyarshost.nasa.gov" });

 # Mark a disk up.
 my $ok = $r->set_status({ root => "/acps/disk/one", state => "up" });

=head1 DESCRIPTION

Client for Yars.

=head1 SEE ALSO

 yarsclient (executable that comes with Yars::Client)
 Clustericious::Client
