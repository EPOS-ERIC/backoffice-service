package org.epos.backoffice.api.controller;
import org.epos.handler.dbapi.service.EntityManagerService;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.logging.Logger;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestcontainersLifecycle {

    protected static Logger LOG = Logger.getGlobal();

    private static EntityManagerService dbService;

    public static PostgreSQLContainer<?> METADATA_CATALOGUE = new PostgreSQLContainer<>(
            DockerImageName.parse("ghcr.io/epos-eric/metadata-database/deploy:main")
                    .asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("cerif")
            .withUsername("postgres")
            .withPassword("changeme")
            .withExposedPorts(5432)
            .withStartupTimeout(Duration.ofMinutes(5))
            .withEnv("POSTGRES_HOST_AUTH_METHOD", "md5")
            .withCommand("postgres", "-c", "password_encryption=md5");

    @BeforeAll
    static void startContainers() {
        METADATA_CATALOGUE.start();

        dbService = new EntityManagerService.EntityManagerServiceBuilder()
                .setConnectionString(METADATA_CATALOGUE.getJdbcUrl())
                .setPostgresqlUsername(METADATA_CATALOGUE.getUsername())
                .setPostgresqlPassword(METADATA_CATALOGUE.getPassword())
                .build();
    }

    @AfterAll
    static void stopContainers() {
        if (dbService != null) {
            LOG.info("Closing EntityManagerService singleton to reset connection pool.");
            dbService.close();
        }

        METADATA_CATALOGUE.stop();
    }
}