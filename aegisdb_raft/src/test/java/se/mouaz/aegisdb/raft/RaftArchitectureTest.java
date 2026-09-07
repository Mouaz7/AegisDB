package se.mouaz.aegisdb.raft;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture tests enforcing structural invariants specified in Section 105 of the assignment.
 * Core database engine and Raft consensus MUST NOT depend on Spring, management,
 * benchmark, or chaos engineering modules.
 */
class RaftArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("se.mouaz.aegisdb.raft");

    @Test
    @DisplayName("Raft consensus core must not depend on Spring Framework")
    void raftMustNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.raft..")
                .should().dependOnClassesThat()
                .resideInAPackage("org.springframework..")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    @DisplayName("Raft consensus core must not depend on management, benchmark, or chaos modules")
    void raftMustNotDependOnManagementOrBenchmarkOrChaos() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("se.mouaz.aegisdb.raft..")
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
