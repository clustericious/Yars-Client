package Yars::Client;

use strict;
use warnings;

use Clustericious::Client;
use Clustericious::Config;
use Mojo::Asset::File;
use Mojo::ByteStream 'b';
use Digest::MD5 qw(md5_hex);
use Mojo::URL;
use Mojo::UserAgent;
use File::Basename;
use File::Spec;
use Log::Log4perl qw(:easy);
use Pod::Usage;

our $VERSION = '0.09';

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
    my $url =
      Mojo::URL->new->scheme("http")->host( $config->host )
      ->port( $config->port );

    return $url;
}

sub content {

    # Like download, but returns the file's content w/o writing to disk.

    my ( $self, $filename, $md5 ) = @_;

    # $dest_dir is an optional destination argument

    unless ( $filename and $md5 ) {
        pod2usage(
            -msg     => "filename and md5 needed for content\n",
            -exitval => 1
        );
    }

    TRACE("content for $filename $md5");
    my $url = $self->_get_url;
    $url->path("/file/$filename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Get the file content
    my $ua      = Mojo::UserAgent->new( max_redirects => 5 );
    my $tx      = $ua->get( $url->to_string );
    my $code    = $tx->res->code;
    my $message = $tx->res->message;

    TRACE("$code:$message");

    LOGWARN 'unable to download file' unless $code == 200;

    return $tx->res->body;
}

sub download {

    # Retrieves a file and saves it.

    my ( $self, $filename, $md5, $dest_dir ) = @_;

    # $dest_dir is an optional destination argument

    unless ( $filename and $md5 ) {
        pod2usage(
            -msg     => "filename and md5 needed for download\n",
            -exitval => 1
        );
    }

    TRACE("downloading $filename $md5");
    my $url = $self->_get_url;
    $url->path("/file/$filename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Get the file content
    my $ua      = Mojo::UserAgent->new( max_redirects => 5 );
    my $tx      = $ua->get( $url->to_string );
    my $code    = $tx->res->code;
    my $message = $tx->res->message;

    TRACE("$code:$message");

    LOGWARN 'unable to download file' unless $code == 200;

    my $out_file = $dest_dir ? $dest_dir . "/$filename" : $filename;
    $tx->res->content->asset->move_to($out_file);

    return "$code:$message";
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
    my $ua      = Mojo::UserAgent->new( max_redirects => 5 );
    my $tx      = $ua->delete($url);
    my $code    = $tx->res->code;
    my $message = $tx->res->message;

    TRACE("$code : $message");

    LOGWARN "error deleting file - $code:$message" unless $code == 200;

    return "$code:$message";
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
    my $md5      = md5_hex($content);

    my $url = $self->_get_url;
    $url->path("/file/$basename/$md5");
    TRACE( "Yars URL: ", $url->to_string );

    # Put the file
    my $ua = Mojo::UserAgent->new( max_redirects => 5 );
    my $tx = $ua->put( $url => $content );
    my $code    = $tx->res->code    || 400;
    my $message = $tx->res->message || 'upload error';

    TRACE("$code : $message");

    unless ( $code == 201 or $code == 409 ) {

        # Warn unless the file was uploaded or found to already exist
        LOGWARN "error uploading file - $code:$message";
    }

    return "$code:$message";
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

Client for Yars.  Yars and Yars-Client are lightweight alternative to RESTAS that can be used during development.


=head1 SEE ALSO

 yarsclient (executable that comes with Yars::Client)
 RESTAS-Client
 Clustericious::Client
