plugins {
    `java`
    application
}

// Só o domínio do core (JobClass, ConsumptionPolicy): a análise lê o JSONL sem os adaptadores.
dependencies {
    implementation(project(":core"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("br.com.filas.experimento.analysis.AnalysisMain")
}

// Caminhos dos registros são relativos à raiz do repositório.
tasks.named<JavaExec>("run") {
    workingDir = rootDir
}
