package se.mouaz.jdistdb.common;

import java.util.Objects;

public record Endpoint(String host, int port) {
    public Endpoint {
        Objects.requireNonNull(host, "host cannot be null");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host cannot be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535, was: " + port);
        }
    }

    public static Endpoint of(String host, int port) {
        return new Endpoint(host, port);
    }

    public static Endpoint from(String address) {
        Objects.requireNonNull(address, "address cannot be null");
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format, expected host:port, got: " + address);
        }
        return new Endpoint(parts[0].trim(), Integer.parseInt(parts[1].trim()));
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
