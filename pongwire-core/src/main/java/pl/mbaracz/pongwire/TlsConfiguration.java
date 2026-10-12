package pl.mbaracz.pongwire;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Server TLS settings, either PEM files or a JDK {@link SSLContext} built by the application.
 */
public final class TlsConfiguration {

    private final Path certificateChain;
    private final Path privateKey;
    private final char[] privateKeyPassword;
    private final SSLContext sslContext;
    private final SSLParameters sslParameters;

    private TlsConfiguration(
        Path certificateChain,
        Path privateKey,
        char[] privateKeyPassword,
        SSLContext sslContext,
        SSLParameters sslParameters
    ) {
        this.certificateChain = certificateChain;
        this.privateKey = privateKey;
        this.privateKeyPassword = privateKeyPassword;
        this.sslContext = sslContext;
        this.sslParameters = sslParameters;
    }

    /**
     * Creates a TLS configuration from an X.509 certificate chain and an unencrypted PKCS#8 private key in PEM format.
     * The files are read again on every start of the server, so a restart picks up renewed certificates.
     *
     * @param certificateChain path to the certificate chain in PEM format.
     * @param privateKey       path to the PKCS#8 private key in PEM format.
     * @return the TLS configuration.
     */
    public static TlsConfiguration forPem(Path certificateChain, Path privateKey) {
        return forPem(certificateChain, privateKey, null);
    }

    /**
     * Creates a TLS configuration from an X.509 certificate chain and a PKCS#8 private key in PEM format.
     * The files are read again on every start of the server, so a restart picks up renewed certificates.
     *
     * @param certificateChain   path to the certificate chain in PEM format.
     * @param privateKey         path to the PKCS#8 private key in PEM format.
     * @param privateKeyPassword password of the private key, or null if it is not encrypted.
     *                           The configuration keeps a copy, so the caller may clear the array.
     * @return the TLS configuration.
     */
    public static TlsConfiguration forPem(Path certificateChain, Path privateKey, char[] privateKeyPassword) {
        return new TlsConfiguration(
            Objects.requireNonNull(certificateChain, "Certificate chain must not be null!"),
            Objects.requireNonNull(privateKey, "Private key must not be null!"),
            privateKeyPassword == null ? null : privateKeyPassword.clone(),
            null,
            null
        );
    }

    /**
     * Creates a TLS configuration from a JDK {@link SSLContext}, e.g. initialized with a {@code KeyManagerFactory}
     * for a PKCS#12 or in-memory key.
     * The key manager decides which certificate a client gets, so an {@code X509ExtendedKeyManager} can select one by
     * the host name the client sends (SNI), or return a renewed certificate without a restart.
     *
     * @param sslContext the TLS context of the server.
     * @return the TLS configuration.
     */
    public static TlsConfiguration forSslContext(SSLContext sslContext) {
        return forSslContext(sslContext, null);
    }

    /**
     * Creates a TLS configuration from a JDK {@link SSLContext} and parameters applied to every connection,
     * e.g. the protocols, the cipher suites, or {@code setNeedClientAuth(true)} for client certificates (mutual TLS).
     * The client certificate is then available through {@link UpgradeRequest#getSslSession()}.
     *
     * @param sslContext    the TLS context of the server.
     * @param sslParameters the parameters of every connection, or null for the defaults of the context.
     *                      The configuration keeps the object, so do not change it afterwards.
     * @return the TLS configuration.
     */
    public static TlsConfiguration forSslContext(SSLContext sslContext, SSLParameters sslParameters) {
        return new TlsConfiguration(
            null,
            null,
            null,
            Objects.requireNonNull(sslContext, "SSL context must not be null!"),
            sslParameters
        );
    }

    Path certificateChain() {
        return certificateChain;
    }

    Path privateKey() {
        return privateKey;
    }

    /**
     * Netty reads PEM keys only with a String password, so the String lives only while the context is built.
     */
    String privateKeyPassword() {
        return privateKeyPassword == null ? null : new String(privateKeyPassword);
    }

    SSLContext sslContext() {
        return sslContext;
    }

    SSLParameters sslParameters() {
        return sslParameters;
    }
}
