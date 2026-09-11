package se.mouaz.aegisdb.transaction;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for the Transaction Engine (Master Project Plan §11).
 * Transaction core must not depend on Spring, gRPC, Protobuf, management, benchmark, or chaos modules.
 */
class TransactionArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.transaction");

    @Test
    @DisplayName("Transaction engine must not depend on gRPC or Protobuf")
    void transactionMustNotDependOnGrpc() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.grpc..", "com.google.protobuf..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Transaction engine must not depend on Spring Framework")
    void transactionMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Transaction engine must not depend on management, benchmark, or chaos modules")
    void transactionMustNotDependOnManagementOrBenchmarkOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.transaction..")
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
