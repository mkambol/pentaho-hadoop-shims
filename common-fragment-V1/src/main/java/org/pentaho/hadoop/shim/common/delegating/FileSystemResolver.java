package org.pentaho.hadoop.shim.common.delegating;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import static java.lang.String.format;

public class FileSystemResolver {

  private static final ConnectionManager connMgr = ConnectionManager.getInstance();
  /**
   * Returns a Hadoop Filesystem.  If the original path is a named pentaho vfs reference
   * (e.g. 'pvfs://HCP_connectionName/some/path'), this method will look up the
   * details of the underlying vfs connection and return a FileSystem configured
   * with the VFS connection info.
   */
  public static FileSystem get( URI uri, Configuration conf ) throws IOException, URISyntaxException {
    Pair pair = resolveUriAndConf( uri, conf );
    return FileSystem.get( pair.uri, pair.conf );
  }

  /**
   * Returns the resolved path.  If the original path is a named pentaho vfs reference
   * (e.g. 'pvfs://connectionName/some/path'), this method will look up the
   * details of the underlying vfs connection and return a Path with the appropriate
   * scheme and host.
   */
  public static Path realPath( String file ) throws URISyntaxException, IOException {
    return new Path( resolveUriAndConf( new URI( file ), new Configuration() ).uri );
  }

  private static Pair resolveUriAndConf( URI uri, Configuration conf )
    throws IOException, URISyntaxException {

    Pair pair = Pair.of( uri, conf );
    if ( uri.getScheme().toLowerCase().startsWith( "pvfs" ) ) {
      ConnectionDetails details = connMgr.getConnectionDetails( uri.getHost() );

      if ( details == null ) {
        throw new IllegalStateException( format( "Could not find named VFS connection:  '%s'", uri.getHost() ) );
      }
      switch ( details.getType() ) {
        case "hcp":
          pair = hcpFileSystem( uri, conf, details );
          break;
        case "s3":
          pair = s3FileSystem( uri, conf, details );
          break;
        default:
          uri = new URI( details.getType(), uri.getHost(), uri.getPath(), "" );
          pair = Pair.of( uri, conf );
      }
    }
    return pair;
  }

  private static Pair s3FileSystem( URI uri, Configuration conf, ConnectionDetails details ) throws URISyntaxException {

    // TODO - extract info from ConnectionDetails
    AWSCredentials credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
    conf.set( "fs.s3a.access.key", credentials.getAWSAccessKeyId() );
    conf.set( "fs.s3a.secret.key", credentials.getAWSSecretKey() );
    if ( credentials instanceof BasicSessionCredentials ) {
      conf.set( "fs.s3a.session.token", ( (BasicSessionCredentials) credentials ).getSessionToken() );
    }
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3.buffer.dir", System.getProperty( "java.io.tmpdir" ) );

    uri = new URI( "s3a", uri.getHost(), uri.getPath(), null );
    return Pair.of( uri, conf );
  }

  private static Pair hcpFileSystem( URI uri, Configuration conf, ConnectionDetails details )
    throws IOException, URISyntaxException {
    Map<String, String> props = getPropsFromConnectionDetails( details );
    String namespace = props.get( "namespace" );
    String port = props.get( "port" );

    String username = props.get( "username" );
    String proxyHost = props.get( "proxyHost" );
    String proxyPort = props.get( "proxyPort" );

    conf.set( "fs.s3a.access.key", Base64.getEncoder().encodeToString( username.getBytes() ) );
    conf.set( "fs.s3a.secret.key", DigestUtils.md5Hex( props.get( "password" ) ) );
    conf.set( "fs.s3a.endpoint", props.get( "tenant" ) + "." + props.get( "host" ) );
    conf.set( "fs.s3a.signing-algorithm", "S3SignerType" );
    conf.set( "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    conf.set( "fs.s3a.connection.ssl.enabled", "true" );
    conf.set( "fs.s3a.attempts.maximum", "3" );
    uri = new URI( "s3a", namespace, uri.getPath(), null );
    return Pair.of( uri, conf );
  }

  private static Map<String, String> getPropsFromConnectionDetails( ConnectionDetails details ) {
    // TODO consider modifiying ConnectionDetails to allow retrieval of props directly.
    Class<? extends ConnectionDetails> clazz = details.getClass();
    return Arrays.stream( clazz.getDeclaredFields() )
      .filter( f -> f.isAnnotationPresent( MetaStoreAttribute.class ) )
      .collect( Collectors.toMap( f -> f.getName(), getConnectionDetail( details ) ) );
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

  private static class Pair {
    URI uri;
    Configuration conf;

    private static Pair of( URI uri, Configuration conf ) {
      Pair p = new Pair();
      p.uri = uri;
      p.conf = conf;
      return p;
    }
  }
}
