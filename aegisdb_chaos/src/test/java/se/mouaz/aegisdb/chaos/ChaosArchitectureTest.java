package se.mouaz.aegisdb.chaos;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for Chaos Engineering (Master Project Plan §11).
 * Chaos module must not depend on Spring Framework or Management module.
 */
class ChaosArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.chaos");

    @Test
    @DisplayName("Chaos module must not depend on Spring Framework")
    void chaosMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.chaos..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Chaos module must not depend on Management module")
    void chaosMustNotDependOnManagement() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.chaos..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("se.mouaz.aegisdb.management..")
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
