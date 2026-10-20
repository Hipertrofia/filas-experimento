plugins {
    `java`
    application
}

dependencies {
    implementation(project(":core"))
    implementation(project(":adapter-local"))
    implementation(project(":adapter-sqs"))
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.elasticmq.rest.sqs)
}

application {
    mainClass.set("br.com.filas.experimento.worker.WorkerMain")
}

// Caminhos das configs de rodada são relativos à raiz do repositório.
tasks.named<JavaExec>("run") {
    workingDir = rootDir
}
