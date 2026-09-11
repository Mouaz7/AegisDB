package se.mouaz.aegisdb.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.ClusterId;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.node.config.AegisConfig;
import se.mouaz.aegisdb.node.config.AegisConfigLoader;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Standalone launcher for AegisDB nodes (Master Project Plan §11).
 * Allows starting a single node from the command line.
 */
public class AegisDbServer {
    private static final Logger log = LoggerFactory.getLogger(AegisDbServer.class);
    private static final String VERSION = "1.0.0";

    public static void main(String[] args) {
        String configFile = null;
        String nodeIdStr = null;

        for (int i = 0; i < args.length; i++) {
            if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                printHelpAndExit(0);
            } else if ("--version".equals(args[i]) || "-v".equals(args[i])) {
                System.out.println("AegisDB Server version " + VERSION);
                System.exit(0);
            } else if ("--config".equals(args[i]) && i + 1 < args.length) {
                configFile = args[++i];
            } else if ("--node-id".equals(args[i]) && i + 1 < args.length) {
                nodeIdStr = args[++i];
            } else {
                System.err.println("Error: Unknown parameter passed: " + args[i]);
                printHelpAndExit(1);
            }
        }

        if (configFile == null || nodeIdStr == null) {
            System.err.println("Error: --config and --node-id are required parameters.");
            printHelpAndExit(1);
        }

        File configPath = new File(configFile);
        if (!configPath.exists() || !configPath.isFile()) {
            System.err.println("Error: Configuration file not found: " + configFile);
            System.exit(2);
        }

        AegisConfig config;
        try {
            config = AegisConfigLoader.load(configFile, nodeIdStr);
        } catch (Exception e) {
            System.err.println("Error: Failed to load configuration: " + e.getMessage());
            System.exit(3);
            return;
        }

        log.info("Bootstrapping AegisDB Node: {}", config.nodeConfig().nodeId());
        
        try {
            DatabaseNode node = NodeBootstrap.createGrpcNode(config.nodeConfig(), config.clusterConfig());
            node.start();
            
            // Add shutdown hook to cleanly stop the node on SIGINT
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping node...");
                node.stop();
                log.info("Node stopped.");
            }));
            
            log.info("Node {} successfully started. Press Ctrl+C to stop.", config.nodeConfig().nodeId());
            
            // Block main thread to keep JVM alive
            Thread.currentThread().join();
        } catch (Exception e) {
            log.error("Failed to start AegisDB node", e);
            System.exit(6);
        }
    }

    private static void printHelpAndExit(int exitCode) {
        System.out.println("Usage: java se.mouaz.aegisdb.node.AegisDbServer [options]");
        System.out.println("Options:");
        System.out.println("  --config <path>   Path to the YAML cluster configuration file (Required)");
        System.out.println("  --node-id <id>    The unique ID of this node, matching an entry in the config (Required)");
        System.out.println("  --help, -h        Show this help message and exit");
        System.out.println("  --version, -v     Show version information and exit");
        System.exit(exitCode);
    }
}
