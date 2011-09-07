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

our $VERSION = '0.28';

# max downloads of 1 GB
$ENV{MOJO_MAX_MESSAGE_SIZE} = 1073741824;

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

route 'bucket_map' => "GET", '/bucket_map';
route 'disk_usage' => "GET", '/usage/files_by_disk';
route 'servers_status' => "GET", '/servers/status';

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

sub retrieve {

    # Like download, but w/o writing to disk.

    my ( $self, $filename, $md5 ) = @_;  # dest_dir is optional

    LOGDIE "filename and md5 needed for file retrieval"
        unless ( $filename and $md5 );

    my $url = $self->_get_url("/file/$filename/$md5");
    TRACE("retrieving $filename $md5 from ", $url->to_string);

    # Get the file
    my $tx      = $self->client->get( $url->to_string );

    if ( !$tx->success ) {
        my ($message, $code) = $tx->error;
        if ($code) {
            ERROR "$code $message response";
        }
        else {
            ERROR "yars connection error";
        }
    }

    return $tx;
}

sub download {

    # Downloads a file and saves it to disk.

    my ( $self, $filename, $md5, $dest_dir ) = @_;

    my $tx = $self->retrieve($filename, $md5);

    unless ($tx->error) {
        my $out_file = $dest_dir ? $dest_dir . "/$filename" : $filename;
        $tx->res->content->asset->move_to($out_file);
    }

    return $tx;
}

sub remove {

    # Removes a file

    my ( $self, $filename, $md5 ) = @_;

    LOGDIE "file and md5 needed for remove"
        unless $filename && $md5;

    my $url = $self->_get_url("/file/$filename/$md5");
    TRACE("removing $filename $md5 from ", $url->to_string);

    # Delete the file
    return $self->client->delete($url);  # returns the transaction

}

# Given an md5, determine the correct server
# using a cached list of bucket->server assignments.
sub _server_for {
    my $self = shift;
    my $md5 = shift;
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
        }
    }


    # Return the transaction
    return $tx;
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

sub welcome {
    my $self = shift;
    return $self->SUPER::welcome(@_) unless $self->server_type eq 'RESTAS';

    # Provides a workaround to get a welcome message from a RESTAS server 
    if ( $self->server_type eq 'RESTAS' ) {
        my $status = $self->status;
        if ( ref $status and $status->{server_hostname} ) {
            return "welcome to RESTAS";
        }
        else {
            return $status;
        }
    }
}

1;

__END__

=head1 NAME

Yars::Client (Yet Another REST Server Client)

=head1 SYNOPSIS

 my $r = Yars::Client->new;

 # Put a file
 $r->upload($filename);

 # Get a file
 $r->download($filename, $md5);
 $r->download($filename, $md5, /tmp);   # download it to the /tmp directory

 # Delete a file
 $r->remove($filename, $md5);



=head1 DESCRIPTION

Client for Yars.  Yars and Yars-Client are lightweight alternatives to RESTAS that can be used during development.  Yars-Client is also compatible with RESTAS-Server.  Each of the above methods returns a Mojo::Transaction::HTTP object.


=head1 SEE ALSO

 yarsclient (executable that comes with Yars::Client)
 RESTAS-Client
 Clustericious::Client
 Mojo::Transaction
 Mojo::Transaction::HTTP
