package br.com.filas.experimento;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(packages = "br.com.filas.experimento", importOptions = ImportOption.DoNotIncludeTests.class)
class CoreArchitectureTest {

    @ArchTest
    static final ArchRule coreSoDependeDoJdk = classes()
            .should().onlyDependOnClassesThat()
            .resideInAnyPackage("java..", "br.com.filas.experimento..")
            .because("o core é Java puro, sem dependências fora do JDK");

    @ArchTest
    static final ArchRule dominioNaoConheceAplicacao = noClasses()
            .that().resideInAPackage("..domain..")
            .should().dependOnClassesThat().resideInAPackage("..application..");

    @ArchTest
    static final ArchRule portasNaoConhecemOsCasosDeUso = classes()
            .that().resideInAPackage("..application.port..")
            .should().onlyDependOnClassesThat()
            .resideInAnyPackage("java..", "..domain..", "..application.port..");
}
