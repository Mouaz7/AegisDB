package se.mouaz.aegisdb.mvcc;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for the MVCC Engine (Master Project Plan §11).
 * MVCC core must not depend on Spring, gRPC, or management modules.
 */
class MvccArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.mvcc");

    @Test
    @DisplayName("MVCC engine must not depend on gRPC or Protobuf")
    void mvccMustNotDependOnGrpc() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.mvcc..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.grpc..", "com.google.protobuf..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("MVCC engine must not depend on Spring Framework")
    void mvccMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.mvcc..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("MVCC engine must not depend on management, benchmark, or chaos modules")
    void mvccMustNotDependOnManagementOrBenchmarkOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.mvcc..")
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
