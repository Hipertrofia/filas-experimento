plugins {
    `java-library`
}

dependencies {
    implementation(project(":core"))
    api(platform(libs.aws.sdk.bom))
    api(libs.aws.sqs)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.elasticmq.rest.sqs)
}
