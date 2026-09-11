package se.mouaz.aegisdb.common;

import java.time.Duration;
import java.util.Objects;

public record NetworkConfiguration(
    Duration connectTimeout,
    Duration requestTimeout,
    boolean tlsEnabled,
    String certChainFilePath,
    String privateKeyFilePath,
    String trustCertCollectionFilePath
) {
    public NetworkConfiguration {
        Objects.requireNonNull(connectTimeout, "connectTimeout cannot be null");
        Objects.requireNonNull(requestTimeout, "requestTimeout cannot be null");
    }

    public static NetworkConfiguration defaultConfiguration() {
        return new NetworkConfiguration(Duration.ofSeconds(2), Duration.ofSeconds(3), false, null, null, null);
    }
}
