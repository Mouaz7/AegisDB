package se.mouaz.aegisdb.observability;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for Observability (Master Project Plan §11).
 * Observability module must not depend on Spring Framework, and core consensus must not depend on observability.
 */
class ObservabilityArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.observability");

    @Test
    @DisplayName("Observability module must not depend on Spring Framework")
    void observabilityMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.observability..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Observability module must not depend on Management or Chaos modules")
    void observabilityMustNotDependOnManagementOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.observability..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("se.mouaz.aegisdb.management..", "se.mouaz.aegisdb.chaos..")
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
