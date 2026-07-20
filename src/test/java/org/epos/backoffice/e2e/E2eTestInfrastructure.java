package org.epos.backoffice.e2e;

import java.time.Duration;

import org.epos.backoffice.Swagger2SpringBoot;
import org.epos.handler.dbapi.service.EntityManagerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(classes = Swagger2SpringBoot.class)
@AutoConfigureMockMvc
abstract class E2eTestInfrastructure {

	private static final PostgreSQLContainer METADATA_CATALOGUE = new PostgreSQLContainer(
			DockerImageName.parse("ghcr.io/epos-eric/metadata-database/deploy:main")
					.asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("cerif")
			.withUsername("postgres")
			.withPassword("changeme")
			.withExposedPorts(5432)
			.withStartupTimeout(Duration.ofMinutes(1))
			.withEnv("POSTGRES_HOST_AUTH_METHOD", "md5")
			.withCommand("postgres", "-c", "password_encryption=md5");

	private static EntityManagerService dbService;

	@Autowired
	protected MockMvc mockMvc;

	@Autowired
	protected ObjectMapper objectMapper;

	@BeforeAll
	static void startMetadataDatabase() {
		METADATA_CATALOGUE.start();
		dbService = new EntityManagerService.EntityManagerServiceBuilder()
				.setConnectionString(METADATA_CATALOGUE.getJdbcUrl())
				.setPostgresqlUsername(METADATA_CATALOGUE.getUsername())
				.setPostgresqlPassword(METADATA_CATALOGUE.getPassword())
				.build();
	}

	@AfterAll
	static void stopMetadataDatabase() {
		if (dbService != null) {
			dbService.close();
		}
		METADATA_CATALOGUE.close();
	}
}
