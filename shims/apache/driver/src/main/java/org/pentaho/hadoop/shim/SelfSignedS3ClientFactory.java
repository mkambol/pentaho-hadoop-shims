package org.pentaho.hadoop.shim;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class SelfSignedS3ClientFactory extends DefaultS3ClientFactory {

  @Override protected AmazonS3 newAmazonS3Client( AWSCredentialsProvider credentials, ClientConfiguration awsConf ) {

    SSLContext sslContext = null;
    try {
      sslContext = SSLContextBuilder.create().loadTrustMaterial( new TrustSelfSignedStrategy() ).build();
      HostnameVerifier allowAllHosts = new NoopHostnameVerifier();
      SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory( sslContext, allowAllHosts );

      awsConf.getApacheHttpClientConfig()
        .setSslSocketFactory( connectionFactory );

    } catch ( NoSuchAlgorithmException | KeyStoreException | KeyManagementException e ) {
      throw new IllegalStateException( e );
    }
    return new AmazonS3Client( credentials, awsConf );
  }
}
