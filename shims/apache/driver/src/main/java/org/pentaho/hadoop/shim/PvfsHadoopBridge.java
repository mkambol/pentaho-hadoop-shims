package org.pentaho.hadoop.shim;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.util.Progressable;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static org.apache.hadoop.fs.Path.SEPARATOR;

public class PvfsHadoopBridge extends FileSystem {

  private Configuration conf;
  private FileSystem fs;

  private static final ConnectionManager connMgr = ConnectionManager.getInstance();

  public PvfsHadoopBridge( Configuration conf ) {
    this.conf = conf;
  }

  public PvfsHadoopBridge() {
    this.conf = new Configuration();
  }


  @Override public String getScheme() {
    return "pvfs";
  }


  @Override public Path makeQualified( Path path ) {
    return super.makeQualified( updatePath( path ) );
  }

  /**
   * Returns a Hadoop Filesystem.  If the original path is a named pentaho vfs reference
   * (e.g. 'pvfs://HCP_connectionName/some/path'), this method will look up the
   * details of the underlying vfs connection and return a FileSystem configured
   * with the VFS connection info.
   */
  @Override public URI getUri() {
    Preconditions.checkArgument( fs != null );
    return fs.getUri();
  }

  @Override public FSDataInputStream open( Path path, int i ) throws IOException {
    return getFs( path ).open( updatePath( path ), i );
  }


  @Override public FSDataOutputStream create( Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                              Progressable progressable ) throws IOException {
    return getFs( path ).create( updatePath( path ), fsPermission, b, i, i1, l, progressable );
  }

  @Override public FSDataOutputStream append( Path path, int i, Progressable progressable ) throws IOException {
    return getFs( path ).append( updatePath( path ), i, progressable );
  }

  @Override public boolean rename( Path path, Path path1 ) throws IOException {
    return getFs( path ).rename( updatePath( path ), updatePath( path1 ) );
  }

  @Override public boolean delete( Path path, boolean b ) throws IOException {
    return getFs( path ).delete( updatePath( path ), b );
  }

  @Override public FileStatus[] listStatus( Path path ) throws IOException {
    return getFs( path ).listStatus( updatePath( path ) );
  }

  @Override public void setWorkingDirectory( Path path ) {
    getFs( path ).setWorkingDirectory( updatePath( path ) );
  }

  @Override public Path getWorkingDirectory() {
    Preconditions.checkArgument( fs != null );
    return fs.getWorkingDirectory();
  }

  @Override public boolean mkdirs( Path path, FsPermission fsPermission ) throws IOException {
    return getFs( path ).mkdirs( updatePath( path ), fsPermission );
  }

  @Override public FileStatus getFileStatus( Path path ) throws IOException {
    return getFs( path ).getFileStatus( updatePath( path ) );
  }


  private static PvfsHadoopBridge.Pair resolveUriAndConf( URI uri, Configuration conf )
    throws URISyntaxException {

    PvfsHadoopBridge.Pair pair;
    ConnectionDetails details = connMgr.getConnectionDetails( uri.getHost() );

    if ( details == null ) {
      throw new IllegalStateException( format( "Could not find named VFS connection:  '%s'", uri.getHost() ) );
    }
    switch ( details.getType() ) {
      case "hcp":
        pair = hcpFileSystem( uri, conf, details );
        break;
      case "s3":
      case "s3a":
      case "s3n":
        pair = s3FileSystem( uri, conf, details );
        break;
      default:
        uri = new URI( details.getType(), uri.getHost(), uri.getPath(), "" );
        pair = PvfsHadoopBridge.Pair.of( uri, conf );
    }
    return pair;
  }


  private static PvfsHadoopBridge.Pair s3FileSystem( URI uri, Configuration conf, ConnectionDetails details )
    throws URISyntaxException {
    Map<String, String> props = getPropsFromConnectionDetails( details );

    String accessKey = props.get( "accessKey" );
    String secretKey = props.get( "secretKey" );
    String sessionToken = props.get( "sessionToken" );
    String credentialsFilePath = props.get( "credentialsFilePath" );
    if ( isNullOrEmpty( accessKey ) || isNullOrEmpty( secretKey ) ) {
      PropertiesFileCredentialsProvider credProvider = new PropertiesFileCredentialsProvider( credentialsFilePath );
      AWSCredentials creds = credProvider.getCredentials();
      accessKey = creds.getAWSAccessKeyId();
      secretKey = creds.getAWSSecretKey();

      if ( creds instanceof BasicSessionCredentials ) {
        sessionToken = ( (BasicSessionCredentials) creds ).getSessionToken();
      }
    }
    conf.set( "fs.s3a.access.key", accessKey );
    conf.set( "fs.s3a.secret.key", secretKey );
    if ( !isNullOrEmpty( sessionToken ) ) {
      // use of session token requires the TemporaryAWSCredentialProvider
      conf.set( "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" );
      conf.set( "fs.s3a.session.token", sessionToken );
    }
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );

    conf.set( "fs.s3.buffer.dir", System.getProperty( "java.io.tmpdir" ) );
    String[] splitPath = uri.getPath().split( "/" );
    Preconditions.checkArgument( splitPath.length > 0 );
    String bucket = splitPath[ 1 ];
    String path = SEPARATOR + Arrays.stream( splitPath ).skip( 2 ).collect( Collectors.joining( SEPARATOR ) );
    uri = new URI( "s3a", bucket, path, null );
    return PvfsHadoopBridge.Pair.of( uri, conf );
  }

  private static PvfsHadoopBridge.Pair hcpFileSystem( URI uri, Configuration conf, ConnectionDetails details )
    throws URISyntaxException {
    Map<String, String> props = getPropsFromConnectionDetails( details );
    String namespace = props.get( "namespace" );
    String port = props.get( "port" );
    String hostPort = props.get( "host" ) + ( port == null ? "" : ":" + port );
    String username = props.get( "username" );
    String proxyHost = props.get( "proxyHost" ); // TODO use proxyHost/Port if defined
    String proxyPort = props.get( "proxyPort" );
    boolean acceptSelfSignedCertificates = Boolean.parseBoolean( props.get( "acceptSelfSignedCertificate" ) );

    conf.set( "fs.s3a.access.key", Base64.getEncoder().encodeToString( username.getBytes() ) );
    conf.set( "fs.s3a.secret.key", DigestUtils.md5Hex( props.get( "password" ) ) );
    conf.set( "fs.s3a.endpoint", props.get( "tenant" ) + "." + hostPort );
    conf.set( "fs.s3a.signing-algorithm", "S3SignerType" );
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );

    if ( acceptSelfSignedCertificates ) {
      conf.set( Constants.S3_CLIENT_FACTORY_IMPL, "org.pentaho.hadoop.shim.SelfSignedS3ClientFactory" );
    }

    uri = new URI( "s3a", namespace, uri.getPath(), null );
    return PvfsHadoopBridge.Pair.of( uri, conf );
  }

  private static Map<String, String> getPropsFromConnectionDetails( ConnectionDetails details ) {
    // TODO consider modifiying ConnectionDetails to allow retrieval of props directly.
    Class<? extends ConnectionDetails> clazz = details.getClass();
    return Arrays.stream( clazz.getDeclaredFields() )
      .filter( f -> f.isAnnotationPresent( MetaStoreAttribute.class ) )
      .collect( Collectors.toMap( Field::getName, getConnectionDetail( details ) ) );
  }

  private static Function<Field, String> getConnectionDetail( ConnectionDetails details ) {
    return f -> {
      try {
        f.setAccessible( true );
        Object val = f.get( details );
        return val == null ? "" : val.toString();
      } catch ( IllegalAccessException e ) {
        throw new IllegalStateException( e );
      }
    };
  }

  private FileSystem getFs( Path path ) {
    if ( fs != null ) {
      return fs;
    }
    try {
      Pair pair = resolveUriAndConf( path.toUri(), conf );
      // FileSystem maintains a cache based on uri and conf, so this should not create new instances
      fs = FileSystem.get( pair.uri, pair.conf );
      return fs;
    } catch ( IOException | URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }

  private Path updatePath( Path path ) {
    if ( !getScheme().equals( path.toUri().getScheme() ) ) {
      return path;
    }
    try {
      Pair pair = resolveUriAndConf( path.toUri(), conf );
      return new Path( pair.uri );
    } catch ( URISyntaxException e ) {
      throw new IllegalStateException( e );
    }
  }


  private static class Pair {
    URI uri;
    Configuration conf;

    private static PvfsHadoopBridge.Pair of( URI uri, Configuration conf ) {
      PvfsHadoopBridge.Pair p = new PvfsHadoopBridge.Pair();
      p.uri = uri;
      p.conf = conf;
      return p;
    }
  }
}
