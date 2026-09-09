package se.mouaz.aegisdb.sharding;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for Sharding & Routing (Master Project Plan §11).
 * Sharding core must not depend on Spring, gRPC, Protobuf, management, benchmark, or chaos modules.
 */
class ShardingArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.sharding");

    @Test
    @DisplayName("Sharding engine must not depend on gRPC or Protobuf")
    void shardingMustNotDependOnGrpc() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.sharding..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.grpc..", "com.google.protobuf..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Sharding engine must not depend on Spring Framework")
    void shardingMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.sharding..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Sharding engine must not depend on management, benchmark, or chaos modules")
    void shardingMustNotDependOnManagementOrBenchmarkOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.sharding..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "se.mouaz.aegisdb.management..",
                        "se.mouaz.aegisdb.benchmark..",
                        "se.mouaz.aegisdb.chaos.."
                )
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
