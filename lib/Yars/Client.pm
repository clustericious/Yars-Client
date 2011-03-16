package Yars::Client;

use strict;
use warnings;

use Clustericious::Client;
use Clustericious::Config;
use Mojo::Asset::File;
use Mojo::ByteStream 'b';
use Mojo::Client;
use Mojo::URL;
use File::Basename;
use File::Spec;
use Log::Log4perl qw(:easy);
use Pod::Usage;

our $VERSION = '0.05';


sub _get_url {

    # Helper to create the Mojo URL object.

    my $config = Clustericious::Config->new('Yars');
    my $url =
      Mojo::URL->new->scheme("http")->host( $config->host )
      ->port( $config->port );

   return $url;
}


sub download {

    # Retrieves a file

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
    my $res     = $self->client->get( $url )->res;
    my $code    = $res->code;
    my $message = $res->message;
    TRACE("$code:$message");

    LOGWARN 'unable to download file' unless $code == 200;

    my $out_file = $dest_dir ? $dest_dir . "/$filename" : $filename;
    open( my $OUTFILE, '>', $out_file )
      || LOGDIE "Could not write to $out_file";


    print $OUTFILE $res->body();

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
    my $res     = $self->client->delete( $url )->res;
    my $code    = $res->code;
    my $message = $res->message;
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
    my $md5      = b($content)->md5_sum->to_string;

    my $url = $self->_get_url;
    $url->path("/file/$basename");
    TRACE( "Yars URL: ", $url->to_string );

    # Put the file
    my $res     = $self->client->put( $url => $content )->res;
    my $code    = $res->code;
    my $message = $res->message;
    TRACE("$code : $message");

    unless ( $code == 201 or $code == 409 ) {

        # Warn unless the file was uploaded or found to already exist
        LOGWARN "error uploading file - $code:$message";
    }

    return "$code:$message";
}

1;

__END__

=head1 filename

Yars::Client (Yet Another RESTAS Client)

=head1 SYNOPSIS

 my $r = Yars::Client->new;

 # Put a file
 $r->upload($filename);

 # Get a file 
 $r->download($filename, $md5);

 # Delete a file
 $r->remove($filename, $md5);


=head1 DESCRIPTION

Client for Yars.  Yars and Yars-Client are lightweight alternative to RESTAS that can be used during development.


=head1 SEE ALSO

 yarsclient (executable that comes with Yars::Client)
 RESTAS-Client
 Clustericious::Client
