package se.mouaz.aegisdb.benchmark;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for Benchmarking (Master Project Plan §11).
 * Benchmark module must not depend on Spring Framework.
 */
class BenchmarkArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.benchmark");

    @Test
    @DisplayName("Benchmark module must not depend on Spring Framework")
    void benchmarkMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.benchmark..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
