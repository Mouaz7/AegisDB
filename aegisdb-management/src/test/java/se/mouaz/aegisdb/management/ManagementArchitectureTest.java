package se.mouaz.aegisdb.management;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants for Management and Security (Master Project Plan §11).
 * Management module must remain decoupled from Chaos module and must not depend on Spring Framework.
 */
class ManagementArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.management");

    @Test
    @DisplayName("Management module must not depend on Chaos module")
    void managementMustNotDependOnChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.management..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("se.mouaz.aegisdb.chaos..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Management module must not depend on Spring Framework")
    void managementMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.management..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
