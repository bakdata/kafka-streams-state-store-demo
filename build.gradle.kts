plugins {
    java
}

group = "com.bakdata.kafka"

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<Test> {
    useJUnitPlatform()
}

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    val junitVersion = "5.9.1"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.23.1")
    val testcontainersVersion = "1.17.6"
    testImplementation(group = "org.testcontainers", name = "kafka", version = testcontainersVersion)
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = testcontainersVersion)
    testImplementation(group = "com.google.guava", name = "guava", version = "31.1-jre")
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = "2.19.0")
}
