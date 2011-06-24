package Yars::Client;

use strict;
use warnings;

use Clustericious::Client;
use Clustericious::Config;
use Mojo::Asset::File;
use Mojo::ByteStream 'b';
use Mojo::URL;
use Mojo::UserAgent;
use File::Basename;
use File::Spec;
use Log::Log4perl qw(:easy);
use Pod::Usage;

our $VERSION = '0.18';

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

route 'welcome' => 'GET', '/';



sub _get_url {

    # Helper to create the Mojo URL object.

    my $config = Clustericious::Config->new('Yars');
    my $url = Mojo::URL->new( $config->url );

    return $url;
}

sub retrieve {

    # Like download, but w/o writing to disk.  Returns the transaction.

    my ( $self, $filename, $md5 ) = @_;  # dest_dir is optional

    unless ( $filename and $md5 ) {
        pod2usage(
            -msg     => "filename and md5 needed for content\n",
            -exitval => 1
        );
    }

    TRACE("retrieving $filename $md5");
    my $url = $self->_get_url->path("/file/$filename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Get the file
    my $tx      = $self->client->get( $url->to_string );

    INFO 'unable to retrieve file' unless $tx->success && $tx->res->code == 200;

    return $tx;
}

sub download {

    # Downloads a file and saves it to disk.  Returns the transaction.

    my ( $self, $filename, $md5, $dest_dir ) = @_;

    my $tx = $self->retrieve($filename, $md5);

    return $tx unless $tx->success;

    my $out_file = $dest_dir ? $dest_dir . "/$filename" : $filename;
    $tx->res->content->asset->move_to($out_file);

    return $tx;
}

sub remove {

    # Removes a file

    my ( $self, $filename, $md5 ) = @_;

    pod2usage(
        -msg     => "file and md5 needed for remove",
        -exitval => 1
    ) unless $filename && $md5;

    my $url = $self->_get_url;
    $url->path("/file/$filename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Delete the file
    return $self->client->delete($url);
}

sub upload {

    # Uploads a file

    my ( $self, $filename ) = @_;

    pod2usage(
        -msg     => "file needed for upload",
        -exitval => 1
    ) unless $filename;
    -r $filename or LOGDIE "Could not read " . File::Spec->rel2abs($filename);

    # Read the file
    my $asset    = Mojo::Asset::File->new( path => $filename );
    my $basename = basename($filename);
    my $content  = $asset->slurp;
    my $md5      = b($content)->md5_sum;

    my $url = $self->_get_url->path("/file/$basename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Return the transaction
    my $tx = $self->client->put( $url => $content );

    if ( my $res = $tx->success ) {
        print $res->code," ",$res->default_message,"\n";
    }
    else {
        my ( $message, $code ) = $tx->error;
        if ($code) {
            print "$code $message response.\n";
        }
        else {
            print "Connection error: $message\n";
        }
    }

    return $tx;
}

1;

__END__

=head1 NAME

Yars::Client (Yet Another RESTAS Client)

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
 Mojo::Transaction::HTTP
 Mojo::Transaction
