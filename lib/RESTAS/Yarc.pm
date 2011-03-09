package RESTAS::Yarc;

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

our $VERSION = '0.03';

route 'download_file' => 'GET', '/file';

route 'remove' => 'DELETE', '/file';

sub download {

    # Retrieves a file

    my ( $self, $filename, $md5 ) = @_;

    unless ( $filename and $md5 ) {
        pod2usage(
            -msg     => "filename and md5 needed for download\n",
            -exitval => 1
        );
    }

    INFO("downloading $filename $md5");
    my $content = $self->download_file( $filename, $md5 );

    if ( !$content ) {
        LOGWARN 'unable to download file';
        return undef;
    }

    open( my $OUTFILE, '>', $filename )
      || LOGDIE "Could not write to $filename";

    print $OUTFILE $content;

    return "file downloaded";
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

    # Define the RESTAS URL
    my $config = Clustericious::Config->new('RESTAS');
    my $url =
      Mojo::URL->new->scheme("http")->host( $config->host )
      ->port( $config->port )->path("/file/$basename/$md5");
    TRACE( "RESTAS URL: ", $url->to_string );

    # Put the file
    my $res     = $self->client->put( $url => $content )->res;
    my $code    = $res->code;
    my $message = $res->message;
    TRACE("$code : $message");

    unless ( $code == 201 or $code == 409 ) {

        # Die unless the file was uploaded or found to already exist
        LOGWARN "error uploading file - $code : $message";
    }

    return "$code : $message";
}

1;

__END__

=head1 filename

RESTAS::Yarc - RESTAS Yarc (Yet Another RESTAS Client)

=head1 SYNOPSIS

 my $r = RESTAS::Yarc->new;

 # Put a file
 $r->upload($filename);

 # Get a file 
 $r->download($filename, $md5);

 # Delete a file
 $r->remove($filename, $md5);


=head1 DESCRIPTION

Perl client for the RESTAS API.  Alternative to RESTAS-Client that uses the Clustericious framework.


=head1 SEE ALSO

 yarc (executable that comes with RESTAS::Yarc)
 RESTAS-Client
 Clustericious::Client
