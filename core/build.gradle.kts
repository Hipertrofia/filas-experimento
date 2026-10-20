plugins {
    `java`
}

// O core não tem dependências de produção: só JDK. Ver CoreArchitectureTest.
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.archunit.junit5)
}
