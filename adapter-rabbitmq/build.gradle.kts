plugins {
    `java-library`
}

dependencies {
    implementation(project(":core"))
    api(libs.amqp.client)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Os testes de integração rodam contra o broker de infra/rabbitmq (ou RABBITMQ_URI) e são
// pulados se ele não estiver no ar.
tasks.withType<Test>().configureEach {
    environment("RABBITMQ_URI", System.getenv("RABBITMQ_URI") ?: "amqp://guest:guest@localhost:5672")
}
