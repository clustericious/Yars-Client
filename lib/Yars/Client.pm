package Yars::Client;

use strict;
use warnings;

use Clustericious::Client;
use Clustericious::Client::Command;
use Clustericious::Config;
use Mojo::Asset::File;
use Mojo::ByteStream 'b';
use Mojo::URL;
use Mojo::UserAgent;
use File::Basename;
use File::Spec;
use Log::Log4perl qw(:easy);
use JSON;
use feature 'say';
use Data::Dumper;

our $VERSION = '0.25';

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

sub upload {

    # Uploads a file

    my ( $self, $filename ) = @_;

    LOGDIE "file needed for upload" unless $filename;
    $filename = File::Spec->rel2abs($filename);
    -r $filename or LOGDIE "Could not read " . $filename;

    # Read the file
    my $basename = basename($filename);
    my $asset    = Mojo::Asset::File->new( path => $filename );
    my $content  = $asset->slurp;
    my $md5      = b($content)->md5_sum;

    my $url = $self->_get_url("/file/$basename/$md5");

    my $tx = $self->client->put( $url => $content );


    if ( my ($message, $code) = $tx->error ) {

        if (defined $code) {
            if ( $self->server_type eq 'RESTAS' and $code == 409 ) {
                # Workaround for RESTAS which sends a 409 instead of a 200 when
                # putting a previously putted file.
                $tx->res->error(undef);  # unset the error flag
                $tx->res->code(200);
                $tx->res->message('ok');
            }
            else {
                ERROR "$code $message";
            }
        }
        else {
            ERROR $message;
        }

    }

    # Return the transaction
    return $tx;
}

sub server_type {
    my $self = shift;
    my $config = Clustericious::Config->new('Yars');
    my $server_type = $config->server_type(default => 'Yars');

    $server_type =~ /RESTAS/i
    ? 'RESTAS'
    : 'Yars';
}

sub status {
    my ($self) = @_;

    # Provides a workaround for getting the status of a RESTAS server.

    if ( $self->server_type eq 'RESTAS' ) {
        # RESTAS sever status

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
            return $tx->error;
        }
    }
    else {
        # Yars server status

        my $url = $self->_get_url();
        my $tx = $self->client->get( $url->to_string . '/status' );
        return $tx->error
            ? $tx->error
            : decode_json( $tx->res->body );
    }
}

sub welcome {

    # Provides a workaround to get a welcome message from a RESTAS server 

    my $self = shift;

    if ( $self->server_type eq 'RESTAS' ) {
        my $status = $self->status;
        if ( ref $status and $status->{server_hostname} ) {
            return "welcome to RESTAS";
        }
        else {
            return $status;
        }
    }
    else {
        my $tx = $self->client->get( $self->server_url);

        return $tx->success
            ? $tx->res->body
            : $tx;
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
