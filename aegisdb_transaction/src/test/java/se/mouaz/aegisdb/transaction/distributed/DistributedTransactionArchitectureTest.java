package se.mouaz.aegisdb.transaction.distributed;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for 2PC Distributed Transactions (Master Project Plan §11; US015).
 */
class DistributedTransactionArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.transaction.distributed");

    @Test
    @DisplayName("Distributed 2PC module must not depend on gRPC or Protobuf")
    void distributedMustNotDependOnGrpc() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction.distributed..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.grpc..", "com.google.protobuf..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Distributed 2PC module must not depend on Spring Framework")
    void distributedMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction.distributed..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Distributed 2PC module must not depend on management, benchmark, or chaos modules")
    void distributedMustNotDependOnManagementOrBenchmarkOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction.distributed..")
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
