package org.epos.backoffice.e2e;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;

import org.epos.backoffice.Swagger2SpringBoot;
import org.epos.backoffice.api.util.AddUserToGroupBean;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.User;
import org.epos.handler.dbapi.service.EntityManagerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import model.RequestStatusType;
import model.RoleType;
import usermanagementapis.UserGroupManagementAPI;

@SpringBootTest(classes = Swagger2SpringBoot.class)
@AutoConfigureMockMvc
class CompleteDataProductLifecycleTest {

	private static final Logger LOG = LoggerFactory.getLogger(CompleteDataProductLifecycleTest.class);

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
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@BeforeAll
	static void startMetadataDatabase() {
		LOG.info("=== Infrastructure: start metadata PostgreSQL container ===");
		METADATA_CATALOGUE.start();
		dbService = new EntityManagerService.EntityManagerServiceBuilder()
				.setConnectionString(METADATA_CATALOGUE.getJdbcUrl())
				.setPostgresqlUsername(METADATA_CATALOGUE.getUsername())
				.setPostgresqlPassword(METADATA_CATALOGUE.getPassword())
				.build();
		LOG.info("Infrastructure ready: metadata database connected");
	}

	@AfterAll
	static void stopMetadataDatabase() {
		LOG.info("=== Infrastructure: stop metadata services ===");
		if (dbService != null) {
			dbService.close();
		}
		METADATA_CATALOGUE.close();
		LOG.info("Infrastructure stopped: metadata database disconnected and container closed");
	}

	@Test
	void setupAdminAndEditorForCompleteDataProductScenario() throws Exception {
		LOG.info("=== Authentication guard: reject metadata request without userId ===");
		mockMvc.perform(get("/dataproduct/all")).andExpect(status().isBadRequest());

		// Use the database-provided ALL group
		LOG.info("=== Users and groups: locate default ALL group ===");
		Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
		assertNotNull(allGroup, "Users and groups: default ALL group must exist");

		// Create explicit users so interceptor auto-creation cannot hide authorization
		// defects.
		LOG.info("=== Users and groups: create explicit admin and editor users ===");
		User admin = new User(
				"complete-dp-admin",
				"Admin",
				"CompleteDataProduct",
				"complete-dp-admin@email.email",
				true);
		User editor = new User(
				"complete-dp-editor",
				"Editor",
				"CompleteDataProduct",
				"complete-dp-editor@email.email",
				false);

		ApiResponseMessage adminCreate = UserManager.createUser(admin, admin);
		ApiResponseMessage editorCreate = UserManager.createUser(editor, admin);
		assertAll(
				"Users and groups: create explicit session users",
				() -> assertEquals(ApiResponseMessage.OK, adminCreate.getCode()),
				() -> assertEquals(ApiResponseMessage.OK, editorCreate.getCode()));

		// Accepted memberships are bootstrapped directly because HTTP authentication
		// needs users first.
		LOG.info("=== Users and groups: assign accepted roles in ALL ===");
		AddUserToGroupBean adminMembership = new AddUserToGroupBean();
		adminMembership.setGroupid(allGroup.getId());
		adminMembership.setUserid(admin.getAuthIdentifier());
		adminMembership.setRole(RoleType.ADMIN.name());
		adminMembership.setStatusType(RequestStatusType.ACCEPTED.name());

		AddUserToGroupBean editorMembership = new AddUserToGroupBean();
		editorMembership.setGroupid(allGroup.getId());
		editorMembership.setUserid(editor.getAuthIdentifier());
		editorMembership.setRole(RoleType.EDITOR.name());
		editorMembership.setStatusType(RequestStatusType.ACCEPTED.name());

		ApiResponseMessage adminMembershipResponse = UserManager.addUserToGroup(adminMembership, admin);
		ApiResponseMessage editorMembershipResponse = UserManager.addUserToGroup(editorMembership, admin);
		assertAll(
				"Users and groups: accepted ALL memberships",
				() -> assertEquals(ApiResponseMessage.OK, adminMembershipResponse.getCode()),
				() -> assertEquals(ApiResponseMessage.OK, editorMembershipResponse.getCode()));

		// Verify persisted state through client-visible HTTP responses before metadata
		// operations.
		LOG.info("=== Users and groups: verify persisted users, roles, and memberships ===");
		mockMvc.perform(get("/user/{instanceId}", admin.getAuthIdentifier())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].authIdentifier").value(admin.getAuthIdentifier()))
				.andExpect(jsonPath("$[0].isAdmin").value(true))
				.andExpect(jsonPath("$[0].groups[0].groupId").value(allGroup.getId()))
				.andExpect(jsonPath("$[0].groups[0].role").value(RoleType.ADMIN.name()));
		mockMvc.perform(get("/user/{instanceId}", editor.getAuthIdentifier())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].authIdentifier").value(editor.getAuthIdentifier()))
				.andExpect(jsonPath("$[0].isAdmin").value(false))
				.andExpect(jsonPath("$[0].groups[0].groupId").value(allGroup.getId()))
				.andExpect(jsonPath("$[0].groups[0].role").value(RoleType.EDITOR.name()));
		mockMvc.perform(get("/group/{instanceId}", allGroup.getId())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].users", hasSize(2)))
				.andExpect(jsonPath("$[0].users[*].userId", containsInAnyOrder(
						admin.getAuthIdentifier(), editor.getAuthIdentifier())))
				.andExpect(jsonPath("$[0].users[*].role", containsInAnyOrder(
						RoleType.ADMIN.name(), RoleType.EDITOR.name())));

		// Every client request carries its intended userId so the interceptor loads
		// that session user.
		LOG.info("=== Authentication: verify admin and editor HTTP sessions ===");
		mockMvc.perform(get("/dataproduct/all")
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk());
		mockMvc.perform(get("/dataproduct/all")
				.queryParam("userId", editor.getAuthIdentifier()))
				.andExpect(status().isOk());

		// The client chooses the target group. All generated IDs, completed metadata,
		// and service-managed fields are supplied by the API during creation.
		LOG.info("=== DataProduct: create empty draft through HTTP ===");
		ObjectNode emptyDataProductPayload = objectMapper.createObjectNode();
		emptyDataProductPayload.set("groups", objectMapper.createArrayNode().add(allGroup.getId()));
		JsonNode created = postMetadata("/dataproduct", emptyDataProductPayload, admin.getAuthIdentifier());
		assertNotNull(created.get("instanceId"), "DataProduct creation: instanceId must be generated");
		assertNotNull(created.get("metaId"), "DataProduct creation: metaId must be generated");
		assertNotNull(created.get("uid"), "DataProduct creation: uid must be generated");
		String instanceId = created.get("instanceId").asText();
		String metaId = created.get("metaId").asText();

		LOG.info("=== DataProduct: retrieve empty draft through HTTP ===");
		MvcResult draftResult = mockMvc.perform(get("/dataproduct/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].instanceId").value(instanceId))
				.andExpect(jsonPath("$[0].metaId").value(metaId))
				.andExpect(jsonPath("$[0].uid").value(created.get("uid").asText()))
				.andExpect(jsonPath("$[0].status").value("DRAFT"))
				.andExpect(jsonPath("$[0].editorId").value(admin.getAuthIdentifier()))
				.andExpect(jsonPath("$[0].groups", containsInAnyOrder(allGroup.getId())))
				.andExpect(jsonPath("$[0].title", hasSize(0)))
				.andExpect(jsonPath("$[0].description", hasSize(0)))
				.andReturn();
		mockMvc.perform(get("/group/{instanceId}", allGroup.getId())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].entities", hasItem(metaId)));

		ObjectNode retrievedDraft = (ObjectNode) objectMapper.readTree(draftResult.getResponse().getContentAsString())
				.get(0);
		ObjectNode dataProductPayload = objectMapper.createObjectNode();
		dataProductPayload.set("instanceId", retrievedDraft.get("instanceId"));
		dataProductPayload.set("metaId", retrievedDraft.get("metaId"));
		dataProductPayload.set("uid", retrievedDraft.get("uid"));
		dataProductPayload.set("status", retrievedDraft.get("status"));
		dataProductPayload.set("groups", retrievedDraft.get("groups"));

		LOG.info("=== DataProduct graph: create Identifier and attach relation ===");
		JsonNode identifier = postMetadata("/identifier", objectMapper.createObjectNode()
				.put("status", "DRAFT")
				.put("type", "DOI")
				.put("identifier", "10.1234/complete-dp"), admin.getAuthIdentifier());
		assertEntityStatus("/identifier", identifier, admin.getAuthIdentifier(), "DRAFT");
		mockMvc.perform(get("/identifier/{metaId}/{instanceId}",
				identifier.get("metaId").asText(), identifier.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].type").value("DOI"))
				.andExpect(jsonPath("$[0].identifier").value("10.1234/complete-dp"));
		addRelation(dataProductPayload, "identifier", identifier);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "identifier", identifier);

		LOG.info("=== DataProduct graph: create Category and attach relation ===");
		JsonNode category = postMetadata("/category", objectMapper.createObjectNode()
				.put("status", "PUBLISHED")
				.put("name", "Seismology")
				.put("description", "Earthquake data"), admin.getAuthIdentifier());
		assertEntityStatus("/category", category, admin.getAuthIdentifier(), "PUBLISHED");
		mockMvc.perform(get("/category/{metaId}/{instanceId}",
				category.get("metaId").asText(), category.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].name").value("Seismology"))
				.andExpect(jsonPath("$[0].description").value("Earthquake data"));
		addRelation(dataProductPayload, "category", category);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "category", category);

		LOG.info("=== DataProduct graph: create Organization and attach publisher relation ===");
		ObjectNode organizationPayload = objectMapper.createObjectNode();
		organizationPayload.put("status", "PUBLISHED");
		organizationPayload.put("acronym", "EPOS");
		organizationPayload.set("legalName", objectMapper.createArrayNode().add("EPOS Publisher"));
		JsonNode publisher = postMetadata("/organization", organizationPayload, admin.getAuthIdentifier());
		assertEntityStatus("/organization", publisher, admin.getAuthIdentifier(), "PUBLISHED");
		mockMvc.perform(get("/organization/{metaId}/{instanceId}",
				publisher.get("metaId").asText(), publisher.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].acronym").value("EPOS"))
				.andExpect(jsonPath("$[0].legalName[0]").value("EPOS Publisher"));
		addRelation(dataProductPayload, "publisher", publisher);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "publisher", publisher);

		LOG.info("=== DataProduct graph: create ContactPoint and attach relation ===");
		ObjectNode contactPointPayload = objectMapper.createObjectNode();
		contactPointPayload.put("status", "PUBLISHED");
		contactPointPayload.put("role", "data manager");
		contactPointPayload.set("email", objectMapper.createArrayNode().add("data@example.org"));
		contactPointPayload.set("language", objectMapper.createArrayNode().add("en"));
		JsonNode contactPoint = postMetadata("/contactpoint", contactPointPayload, admin.getAuthIdentifier());
		assertEntityStatus("/contactpoint", contactPoint, admin.getAuthIdentifier(), "PUBLISHED");
		mockMvc.perform(get("/contactpoint/{metaId}/{instanceId}",
				contactPoint.get("metaId").asText(), contactPoint.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].role").value("data manager"))
				.andExpect(jsonPath("$[0].email[0]").value("data@example.org"))
				.andExpect(jsonPath("$[0].language[0]").value("en"));
		addRelation(dataProductPayload, "contactPoint", contactPoint);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "contactPoint", contactPoint);

		LOG.info("=== DataProduct graph: create Location and attach spatial extent ===");
		JsonNode location = postMetadata("/location", objectMapper.createObjectNode()
				.put("status", "DRAFT")
				.put("location", "Mediterranean region"), admin.getAuthIdentifier());
		assertEntityStatus("/location", location, admin.getAuthIdentifier(), "DRAFT");
		mockMvc.perform(get("/location/{metaId}/{instanceId}",
				location.get("metaId").asText(), location.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].location").value("Mediterranean region"));
		addRelation(dataProductPayload, "spatialExtent", location);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "spatialExtent", location);

		LOG.info("=== DataProduct graph: create PeriodOfTime and attach temporal extent ===");
		JsonNode periodOfTime = postMetadata("/periodoftime", objectMapper.createObjectNode()
				.put("status", "DRAFT")
				.put("startDate", "2020-01-01T00:00:00Z")
				.put("endDate", "2020-12-31T23:59:59Z"), admin.getAuthIdentifier());
		assertEntityStatus("/periodoftime", periodOfTime, admin.getAuthIdentifier(), "DRAFT");
		mockMvc.perform(get("/periodoftime/{metaId}/{instanceId}",
				periodOfTime.get("metaId").asText(), periodOfTime.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].startDate").value(startsWith("2020-01-01")))
				.andExpect(jsonPath("$[0].endDate").value(startsWith("2020-12-31")));
		addRelation(dataProductPayload, "temporalExtent", periodOfTime);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelation(metaId, instanceId, admin.getAuthIdentifier(), "temporalExtent", periodOfTime);

		LOG.info("=== DataProduct graph: create Operation ===");
		ObjectNode operationPayload = objectMapper.createObjectNode();
		operationPayload.put("status", "DRAFT");
		operationPayload.put("method", "GET");
		operationPayload.put("template", "https://api.example.org/data");
		JsonNode operation = postMetadata("/operation", operationPayload, admin.getAuthIdentifier());
		assertEntityStatus("/operation", operation, admin.getAuthIdentifier(), "DRAFT");
		mockMvc.perform(get("/operation/{metaId}/{instanceId}",
				operation.get("metaId").asText(), operation.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].method").value("GET"))
				.andExpect(jsonPath("$[0].template").value("https://api.example.org/data"));

		LOG.info("=== DataProduct graph: create WebService linked to Operation ===");
		ObjectNode webServicePayload = objectMapper.createObjectNode();
		webServicePayload.put("status", "DRAFT");
		webServicePayload.put("name", "Complete Data API");
		webServicePayload.put("description", "DataProduct test API");
		webServicePayload.put("entryPoint", "https://api.example.org");
		webServicePayload.set("supportedOperation", objectMapper.createArrayNode().add(linkedEntity(operation)));
		JsonNode webService = postMetadata("/webservice", webServicePayload, admin.getAuthIdentifier());
		assertEntityStatus("/webservice", webService, admin.getAuthIdentifier(), "DRAFT");

		LOG.info("=== DataProduct graph: create Distribution linked to WebService and Operation ===");
		ObjectNode distributionPayload = objectMapper.createObjectNode();
		distributionPayload.put("status", "DRAFT");
		distributionPayload.set("title", objectMapper.createArrayNode().add("Complete Data Distribution"));
		distributionPayload.set("accessURL", objectMapper.createArrayNode().add("https://api.example.org/data"));
		distributionPayload.set("accessService", objectMapper.createArrayNode().add(linkedEntity(webService)));
		distributionPayload.set("supportedOperation", objectMapper.createArrayNode().add(linkedEntity(operation)));
		JsonNode distribution = postMetadata("/distribution", distributionPayload, admin.getAuthIdentifier());
		assertEntityStatus("/distribution", distribution, admin.getAuthIdentifier(), "DRAFT");
		addRelation(dataProductPayload, "distribution", distribution);
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(), identifier, category, publisher,
				contactPoint, location, periodOfTime, distribution);
		mockMvc.perform(get("/webservice/{metaId}/{instanceId}",
				webService.get("metaId").asText(), webService.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].name").value(webServicePayload.get("name").asText()))
				.andExpect(jsonPath("$[0].description").value(webServicePayload.get("description").asText()))
				.andExpect(jsonPath("$[0].entryPoint").value(webServicePayload.get("entryPoint").asText()))
				.andExpect(jsonPath("$[0].supportedOperation[*].instanceId",
						hasItem(operation.get("instanceId").asText())));

		LOG.info("=== DataProduct fields: add title through PUT ===");
		dataProductPayload.set("title", objectMapper.createArrayNode().add("Complete DataProduct"));
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add description through PUT ===");
		dataProductPayload.set("description", objectMapper.createArrayNode().add("Complete DataProduct description"));
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add keywords through PUT ===");
		dataProductPayload.put("keywords", "seismology,earthquakes");
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add classification and documentation through PUT ===");
		dataProductPayload.put("type", "http://purl.org/dc/dcmitype/Collection");
		dataProductPayload.put("accrualPeriodicity", "daily");
		dataProductPayload.put("accessRight", "open data");
		dataProductPayload.put("documentation", "https://example.org/documentation");
		dataProductPayload.put("qualityAssurance", "https://example.org/quality");
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add stable timestamps through PUT ===");
		dataProductPayload.put("created", "2024-01-02T03:04:05Z");
		dataProductPayload.put("issued", "2024-01-03T03:04:05Z");
		dataProductPayload.put("modified", "2024-01-04T03:04:05Z");
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add version and provenance through PUT ===");
		dataProductPayload.put("versionInfo", "1.0");
		dataProductPayload.set("provenance", objectMapper.createArrayNode().add("https://example.org/provenance"));
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);

		LOG.info("=== DataProduct fields: add links and measured variable through PUT ===");
		dataProductPayload.set("landingPage", objectMapper.createArrayNode().add("https://example.org/data"));
		dataProductPayload.set("referencedBy", objectMapper.createArrayNode().add("https://example.org/reference"));
		dataProductPayload.set("variableMeasured", objectMapper.createArrayNode().add("ground motion"));
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(),
				identifier, category, publisher, contactPoint, location, periodOfTime, distribution);
		assertDataProductFields(metaId, instanceId, admin.getAuthIdentifier(), created.get("uid").asText(),
				allGroup.getId());
		mockMvc.perform(get("/distribution/{metaId}/{instanceId}",
				distribution.get("metaId").asText(), distribution.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].title[0]").value(distributionPayload.get("title").get(0).asText()))
				.andExpect(jsonPath("$[0].accessURL[0]").value(distributionPayload.get("accessURL").get(0).asText()))
				.andExpect(jsonPath("$[0].accessService[*].instanceId",
						hasItem(webService.get("instanceId").asText())))
				.andExpect(jsonPath("$[0].supportedOperation[*].instanceId",
						hasItem(operation.get("instanceId").asText())));

		LOG.info("=== DataProduct lifecycle: submit completed draft ===");
		dataProductPayload.put("status", "SUBMITTED");
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductFields(metaId, instanceId, admin.getAuthIdentifier(), created.get("uid").asText(),
				allGroup.getId(), "SUBMITTED");
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(), identifier, category, publisher,
				contactPoint, location, periodOfTime, distribution, "SUBMITTED");

		LOG.info("=== DataProduct lifecycle: publish submitted draft ===");
		dataProductPayload.put("status", "PUBLISHED");
		updateMetadata("/dataproduct", dataProductPayload, admin.getAuthIdentifier());
		assertDataProductFields(metaId, instanceId, admin.getAuthIdentifier(), created.get("uid").asText(),
				allGroup.getId(), "PUBLISHED");
		assertDataProductRelations(metaId, instanceId, admin.getAuthIdentifier(), identifier, category, publisher,
				contactPoint, location, periodOfTime, distribution, "PUBLISHED");

		// Keep track of the published baseline object for future reference
		ObjectNode publishedBaseline = objectMapper.createObjectNode();
		publishedBaseline.set("dataProduct", retrieveEntity("/dataproduct", metaId, instanceId,
				admin.getAuthIdentifier()));
		publishedBaseline.set("identifier", retrieveEntity("/identifier", identifier, admin.getAuthIdentifier()));
		publishedBaseline.set("category", retrieveEntity("/category", category, admin.getAuthIdentifier()));
		publishedBaseline.set("publisher", retrieveEntity("/organization", publisher, admin.getAuthIdentifier()));
		publishedBaseline.set("contactPoint", retrieveEntity("/contactpoint", contactPoint, admin.getAuthIdentifier()));
		publishedBaseline.set("location", retrieveEntity("/location", location, admin.getAuthIdentifier()));
		publishedBaseline.set("periodOfTime", retrieveEntity("/periodoftime", periodOfTime, admin.getAuthIdentifier()));
		publishedBaseline.set("operation", retrieveEntity("/operation", operation, admin.getAuthIdentifier()));
		publishedBaseline.set("webService", retrieveEntity("/webservice", webService, admin.getAuthIdentifier()));
		publishedBaseline.set("distribution", retrieveEntity("/distribution", distribution, admin.getAuthIdentifier()));
		assertEquals("PUBLISHED", publishedBaseline.get("dataProduct").get("status").asText());

		LOG.info("=== DataProduct lifecycle: create draft as editor from published version ===");
		ObjectNode editorDraftPayload = publishedBaseline.get("dataProduct").deepCopy();
		editorDraftPayload.put("status", "DRAFT");
		JsonNode editorDraftLink = postMetadata("/dataproduct", editorDraftPayload, editor.getAuthIdentifier());
		assertNotNull(editorDraftLink.get("instanceId"));
		assertNotNull(editorDraftLink.get("metaId"));
		assertNotNull(editorDraftLink.get("uid"));
		assertEquals(publishedBaseline.get("dataProduct").get("metaId"), editorDraftLink.get("metaId"));
		assertEquals(publishedBaseline.get("dataProduct").get("uid"), editorDraftLink.get("uid"));
		JsonNode editorDraft = retrieveEntity("/dataproduct", editorDraftLink.get("metaId").asText(),
				editorDraftLink.get("instanceId").asText(), editor.getAuthIdentifier());
		assertEquals(publishedBaseline.get("dataProduct").get("instanceId"), editorDraft.get("instanceChangedId"));
		assertEquals("DRAFT", editorDraft.get("status").asText());
		assertEquals(editor.getAuthIdentifier(), editorDraft.get("editorId").asText());
		assertEquals(allGroup.getId(), editorDraft.get("groups").get(0).asText());
		assertNotEquals(publishedBaseline.get("dataProduct").get("instanceId"), editorDraft.get("instanceId"));
		assertEquals(publishedBaseline.get("dataProduct"),
				retrieveEntity("/dataproduct", publishedBaseline.get("dataProduct").get("metaId").asText(),
						publishedBaseline.get("dataProduct").get("instanceId").asText(), admin.getAuthIdentifier()));

		LOG.info("=== DataProduct lifecycle: verify automatic child drafting ===");
		JsonNode draftIdentifier = editorDraft.get("identifier").get(0);
		JsonNode draftCategory = editorDraft.get("category").get(0);
		JsonNode draftPublisher = editorDraft.get("publisher").get(0);
		JsonNode draftContactPoint = editorDraft.get("contactPoint").get(0);
		JsonNode draftLocation = editorDraft.get("spatialExtent").get(0);
		JsonNode draftPeriodOfTime = editorDraft.get("temporalExtent").get(0);
		JsonNode draftDistributionLink = editorDraft.get("distribution").get(0);
		JsonNode draftDistribution = retrieveEntity("/distribution", draftDistributionLink, editor.getAuthIdentifier());
		JsonNode draftOperationLink = draftDistribution.get("supportedOperation").get(0);
		JsonNode draftOperation = retrieveEntity("/operation", draftOperationLink, editor.getAuthIdentifier());
		JsonNode draftWebServiceLink = draftDistribution.get("accessService").get(0);
		JsonNode draftWebService = retrieveEntity("/webservice", draftWebServiceLink, editor.getAuthIdentifier());

		assertDraftEntity("/identifier", draftIdentifier, publishedBaseline.get("identifier"),
				editor.getAuthIdentifier(), allGroup.getId());
		assertDraftEntity("/periodoftime", draftPeriodOfTime, publishedBaseline.get("periodOfTime"),
				editor.getAuthIdentifier(), allGroup.getId());
		assertDraftEntity("/location", draftLocation, publishedBaseline.get("location"), editor.getAuthIdentifier(),
				allGroup.getId());
		assertDraftEntity("/distribution", draftDistribution, publishedBaseline.get("distribution"),
				editor.getAuthIdentifier(), allGroup.getId());
		assertDraftEntity("/operation", draftOperation, publishedBaseline.get("operation"), editor.getAuthIdentifier(),
				allGroup.getId());
		assertDraftEntity("/webservice", draftWebService, publishedBaseline.get("webService"),
				editor.getAuthIdentifier(), allGroup.getId());
		assertPublishedEntityLink(draftCategory, publishedBaseline.get("category"));
		assertPublishedEntityLink(draftPublisher, publishedBaseline.get("publisher"));
		assertPublishedEntityLink(draftContactPoint, publishedBaseline.get("contactPoint"));

		assertRelationContainsInstance(editorDraft, "identifier", draftIdentifier);
		assertRelationContainsInstance(editorDraft, "category", draftCategory);
		assertRelationContainsInstance(editorDraft, "publisher", draftPublisher);
		assertRelationContainsInstance(editorDraft, "contactPoint", draftContactPoint);
		assertRelationContainsInstance(editorDraft, "spatialExtent", draftLocation);
		assertRelationContainsInstance(editorDraft, "temporalExtent", draftPeriodOfTime);
		assertRelationContainsInstance(editorDraft, "distribution", draftDistribution);
		assertRelationContainsInstance(draftDistribution, "supportedOperation", draftOperation);
		assertRelationContainsInstance(draftDistribution, "accessService", draftWebService);
		assertRelationContainsInstance(draftOperation, "webservice", draftWebService);
		assertRelationContainsInstance(draftWebService, "supportedOperation", draftOperation);
		assertRelationContainsInstance(draftWebService, "distribution", draftDistribution);

		assertEquals(publishedBaseline.get("identifier"),
				retrieveEntity("/identifier", identifier, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("category"),
				retrieveEntity("/category", category, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("publisher"),
				retrieveEntity("/organization", publisher, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("contactPoint"),
				retrieveEntity("/contactpoint", contactPoint, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("location"),
				retrieveEntity("/location", location, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("periodOfTime"),
				retrieveEntity("/periodoftime", periodOfTime, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("operation"),
				retrieveEntity("/operation", operation, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("webService"),
				retrieveEntity("/webservice", webService, admin.getAuthIdentifier()));
		assertEquals(publishedBaseline.get("distribution"),
				retrieveEntity("/distribution", distribution, admin.getAuthIdentifier()));

		LOG.info("=== DataProduct lifecycle: modify DataProduct fields ===");
		ObjectNode modifiedDataProductPayload = editorDraft.deepCopy();
		modifiedDataProductPayload.set("title", objectMapper.createArrayNode().add("Edited Complete DataProduct"));
		modifiedDataProductPayload.set("description",
				objectMapper.createArrayNode().add("Edited DataProduct description"));
		modifiedDataProductPayload.put("keywords", "edited,seismology");
		modifiedDataProductPayload.put("modified", "2024-02-04T05:06:07Z");
		modifiedDataProductPayload.put("versionInfo", "2.0");
		updateMetadata("/dataproduct", modifiedDataProductPayload, editor.getAuthIdentifier());
		JsonNode modifiedDataProduct = retrieveEntity("/dataproduct", editorDraft.get("metaId").asText(),
				editorDraft.get("instanceId").asText(), editor.getAuthIdentifier());
		assertAll("DataProduct field update",
				() -> assertEquals("Edited Complete DataProduct", modifiedDataProduct.get("title").get(0).asText(),
						"title must be updated"),
				() -> assertEquals("Edited DataProduct description",
						modifiedDataProduct.get("description").get(0).asText(),
						"description must be updated"),
				() -> assertEquals("edited,seismology", modifiedDataProduct.get("keywords").asText(),
						"keywords must be updated"),
				() -> assertTrue(modifiedDataProduct.get("modified").asText().startsWith("2024-02-04T05:06:07"),
						"modified timestamp must be updated"),
				() -> assertEquals("2.0", modifiedDataProduct.get("versionInfo").asText(),
						"versionInfo must be updated"),
				() -> assertEquals(editorDraft.get("type"), modifiedDataProduct.get("type"),
						"type must be preserved"),
				() -> assertEquals(editorDraft.get("accessRight"), modifiedDataProduct.get("accessRight"),
						"accessRight must be preserved"));
		assertRelationContainsInstance(modifiedDataProduct, "identifier", draftIdentifier);
		assertRelationContainsInstance(modifiedDataProduct, "distribution", draftDistribution);
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: modify Distribution fields ===");
		ObjectNode modifiedDistributionPayload = entityUpdatePayload(draftDistribution);
		modifiedDistributionPayload.set("title",
				objectMapper.createArrayNode().add("Edited Complete Data Distribution"));
		modifiedDistributionPayload.set("accessURL",
				objectMapper.createArrayNode().add("https://api.example.org/edited-data"));
		modifiedDistributionPayload.set("description",
				objectMapper.createArrayNode().add("Edited distribution description"));
		// actually it seems that it's not needed to also pass the webservice and
		// operation every time, as the instanceChangedId helps us retrieve the
		// associated entities and preserve the relations. we still pass them here
		// because that's what the current client does
		modifiedDistributionPayload.set("accessService",
				objectMapper.createArrayNode().add(linkedEntity(draftWebServiceLink)));
		modifiedDistributionPayload.set("supportedOperation",
				objectMapper.createArrayNode().add(linkedEntity(draftOperationLink)));
		updateMetadata("/distribution", modifiedDistributionPayload, editor.getAuthIdentifier());
		JsonNode modifiedDistribution = retrieveEntity("/distribution", draftDistribution, editor.getAuthIdentifier());
		assertAll("Distribution field update",
				() -> assertEquals("Edited Complete Data Distribution",
						modifiedDistribution.get("title").get(0).asText(),
						"Distribution title must be updated"),
				() -> assertEquals("https://api.example.org/edited-data",
						modifiedDistribution.get("accessURL").get(0).asText(),
						"Distribution accessURL must be updated"),
				() -> assertEquals("Edited distribution description",
						modifiedDistribution.get("description").get(0).asText(),
						"Distribution description must be updated"));
		assertRelationContainsInstance(modifiedDistribution, "supportedOperation", draftOperation);
		assertRelationContainsInstance(modifiedDistribution, "accessService", draftWebService);
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: modify WebService fields ===");
		ObjectNode modifiedWebServicePayload = entityUpdatePayload(draftWebService);
		modifiedWebServicePayload.put("name", "Edited Complete Data API");
		modifiedWebServicePayload.put("description", "Edited DataProduct test API");
		modifiedWebServicePayload.put("entryPoint", "https://api.example.org/edited");
		// actually it seems that it's not needed to also pass the webservice and
		// operation every time, as the instanceChangedId helps us retrieve the
		// associated entities and preserve the relations. we still pass them here
		// because that's what the current client does
		modifiedWebServicePayload.set("supportedOperation",
				objectMapper.createArrayNode().add(linkedEntity(draftOperationLink)));
		modifiedWebServicePayload.set("distribution",
				objectMapper.createArrayNode().add(linkedEntity(draftDistributionLink)));
		updateMetadata("/webservice", modifiedWebServicePayload, editor.getAuthIdentifier());
		JsonNode modifiedWebService = retrieveEntity("/webservice", draftWebService, editor.getAuthIdentifier());
		assertAll("WebService field update",
				() -> assertEquals("Edited Complete Data API", modifiedWebService.get("name").asText(),
						"WebService name must be updated"),
				() -> assertEquals("Edited DataProduct test API", modifiedWebService.get("description").asText(),
						"WebService description must be updated"),
				() -> assertEquals("https://api.example.org/edited", modifiedWebService.get("entryPoint").asText(),
						"WebService entryPoint must be updated"));
		assertRelationContainsInstance(modifiedWebService, "supportedOperation", draftOperation);
		assertRelationContainsInstance(modifiedWebService, "distribution", draftDistribution);
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: modify Operation fields ===");
		ObjectNode modifiedOperationPayload = entityUpdatePayload(draftOperation);
		modifiedOperationPayload.put("method", "POST");
		modifiedOperationPayload.put("template", "https://api.example.org/edited-data");
		// actually it seems that it's not needed to also pass the webservice every
		// time, as the instanceChangedId helps us retrieve the associated entities and
		// preserve the relations. we still pass it here because that's what the current
		// client does
		modifiedOperationPayload.set("webservice",
				objectMapper.createArrayNode().add(linkedEntity(draftWebServiceLink)));
		updateMetadata("/operation", modifiedOperationPayload, editor.getAuthIdentifier());
		JsonNode modifiedOperation = retrieveEntity("/operation", draftOperation, editor.getAuthIdentifier());
		assertAll("Operation field update",
				() -> assertEquals("POST", modifiedOperation.get("method").asText(),
						"Operation method must be updated"),
				() -> assertEquals("https://api.example.org/edited-data", modifiedOperation.get("template").asText(),
						"Operation template must be updated"));
		assertRelationContainsInstance(modifiedOperation, "webservice", draftWebService);
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: add second Distribution ===");
		ObjectNode additionalDistributionPayload = objectMapper.createObjectNode();
		additionalDistributionPayload.put("status", "DRAFT");
		additionalDistributionPayload.set("groups", modifiedDataProduct.get("groups"));
		JsonNode additionalDistribution = postMetadata("/distribution", additionalDistributionPayload,
				editor.getAuthIdentifier());
		assertEntityStatus("/distribution", additionalDistribution, editor.getAuthIdentifier(), "DRAFT");

		ObjectNode dataProductWithAdditionalDistribution = modifiedDataProduct.deepCopy();
		dataProductWithAdditionalDistribution.set("distribution", objectMapper.createArrayNode()
				.add(linkedEntity(draftDistributionLink)).add(linkedEntity(additionalDistribution)));
		updateMetadata("/dataproduct", dataProductWithAdditionalDistribution, editor.getAuthIdentifier());
		JsonNode dataProductWithAdditionalDistributionResult = retrieveEntity("/dataproduct", editorDraft,
				editor.getAuthIdentifier());
		assertEquals(2, dataProductWithAdditionalDistributionResult.get("distribution").size());
		assertTrue(dataProductWithAdditionalDistributionResult.get("distribution").findValuesAsText("instanceId")
				.contains(draftDistribution.get("instanceId").asText()));
		assertTrue(dataProductWithAdditionalDistributionResult.get("distribution").findValuesAsText("instanceId")
				.contains(additionalDistribution.get("instanceId").asText()));
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: clear Distribution URL list ===");
		ObjectNode clearedDistributionPayload = entityUpdatePayload(modifiedDistribution);
		clearedDistributionPayload.set("title", modifiedDistribution.get("title"));
		clearedDistributionPayload.set("description", modifiedDistribution.get("description"));
		// actually it seems that it's not needed to also pass the webservice and
		// operation every time, as the instanceChangedId helps us retrieve the
		// associated entities and preserve the relations. we still pass them here
		// because that's what the current client does
		clearedDistributionPayload.set("accessService",
				objectMapper.createArrayNode().add(linkedEntity(draftWebServiceLink)));
		clearedDistributionPayload.set("supportedOperation",
				modifiedDistribution.get("supportedOperation"));
		clearedDistributionPayload.set("accessURL", objectMapper.createArrayNode());
		updateMetadata("/distribution", clearedDistributionPayload, editor.getAuthIdentifier());
		JsonNode clearedDistribution = retrieveEntity("/distribution", draftDistribution, editor.getAuthIdentifier());
		assertEquals(0, clearedDistribution.get("accessURL").size());
		assertRelationContainsInstance(clearedDistribution, "supportedOperation", draftOperation);
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: submit draft ===");
		JsonNode editorDraftBeforeSubmit = retrieveEntity("/dataproduct", editorDraft, editor.getAuthIdentifier());
		ObjectNode submitEditorDraftPayload = editorDraftBeforeSubmit.deepCopy();
		submitEditorDraftPayload.put("status", "SUBMITTED");
		updateMetadata("/dataproduct", submitEditorDraftPayload, editor.getAuthIdentifier());
		JsonNode submittedEditorDraft = retrieveEntity("/dataproduct", editorDraft, editor.getAuthIdentifier());
		assertAll("Draft submission",
				() -> assertEquals("SUBMITTED", submittedEditorDraft.get("status").asText(),
						"editor DataProduct must be SUBMITTED"),
				() -> assertEquals(editorDraft.get("instanceId"), submittedEditorDraft.get("instanceId"),
						"submission must preserve DataProduct instanceId"),
				() -> assertEquals(publishedBaseline.get("dataProduct").get("metaId"),
						submittedEditorDraft.get("metaId"),
						"submission must preserve DataProduct metaId"),
				() -> assertEquals(publishedBaseline.get("dataProduct").get("uid"), submittedEditorDraft.get("uid"),
						"submission must preserve DataProduct uid"),
				() -> assertEquals("Edited Complete DataProduct", submittedEditorDraft.get("title").get(0).asText(),
						"submitted title must be preserved"),
				() -> assertEquals("Edited DataProduct description",
						submittedEditorDraft.get("description").get(0).asText(),
						"submitted description must be preserved"),
				() -> assertEquals("edited,seismology", submittedEditorDraft.get("keywords").asText(),
						"submitted keywords must be preserved"),
				() -> assertEquals("2.0", submittedEditorDraft.get("versionInfo").asText(),
						"submitted versionInfo must be preserved"),
				() -> assertEquals(2, submittedEditorDraft.get("distribution").size(),
						"submitted DataProduct must retain both Distributions"),
				() -> assertTrue(submittedEditorDraft.get("distribution").findValuesAsText("instanceId")
						.contains(draftDistribution.get("instanceId").asText()),
						"submitted DataProduct must retain original Distribution"),
				() -> assertTrue(submittedEditorDraft.get("distribution").findValuesAsText("instanceId")
						.contains(additionalDistribution.get("instanceId").asText()),
						"submitted DataProduct must retain added Distribution"));
		assertEquals("SUBMITTED", retrieveEntity("/distribution", draftDistribution, editor.getAuthIdentifier())
				.get("status").asText());
		assertEquals("SUBMITTED", retrieveEntity("/distribution", additionalDistribution, editor.getAuthIdentifier())
				.get("status").asText());
		assertEquals("SUBMITTED", retrieveEntity("/webservice", draftWebService, editor.getAuthIdentifier())
				.get("status").asText());
		assertEquals("SUBMITTED", retrieveEntity("/operation", draftOperation, editor.getAuthIdentifier())
				.get("status").asText());
		assertPublishedGraphUnchanged(publishedBaseline, admin.getAuthIdentifier());

		LOG.info("=== DataProduct lifecycle: publish draft ===");
		ObjectNode publishEditorDraftPayload = submittedEditorDraft.deepCopy();
		publishEditorDraftPayload.put("status", "PUBLISHED");
		updateMetadata("/dataproduct", publishEditorDraftPayload, admin.getAuthIdentifier());
		JsonNode finalDataProduct = retrieveEntity("/dataproduct", editorDraft, admin.getAuthIdentifier());
		assertAll("Publication check",
				() -> assertEquals("PUBLISHED", finalDataProduct.get("status").asText(),
						"final DataProduct must be PUBLISHED"),
				() -> assertEquals(editorDraft.get("instanceId"), finalDataProduct.get("instanceId"),
						"publication must preserve editor draft instanceId"),
				() -> assertEquals(publishedBaseline.get("dataProduct").get("metaId"), finalDataProduct.get("metaId"),
						"publication must preserve DataProduct metaId"),
				() -> assertEquals(publishedBaseline.get("dataProduct").get("uid"), finalDataProduct.get("uid"),
						"publication must preserve DataProduct uid"),
				() -> assertEquals(publishedBaseline.get("dataProduct").get("instanceId"),
						finalDataProduct.get("instanceChangedId"),
						"final DataProduct lineage must point to previous published instance"));

		JsonNode finalDistributionLink = findRelationByUid(finalDataProduct, "distribution",
				draftDistribution.get("uid").asText());
		JsonNode finalAdditionalDistributionLink = findRelationByUid(finalDataProduct, "distribution",
				additionalDistribution.get("uid").asText());
		assertNotNull(finalDistributionLink);
		assertNotNull(finalAdditionalDistributionLink);
		JsonNode finalDistribution = retrieveEntity("/distribution", finalDistributionLink, admin.getAuthIdentifier());
		JsonNode finalAdditionalDistribution = retrieveEntity("/distribution", finalAdditionalDistributionLink,
				admin.getAuthIdentifier());
		assertAll("Final Distribution graph",
				() -> assertEquals("PUBLISHED", finalDistribution.get("status").asText(),
						"modified Distribution must be PUBLISHED"),
				() -> assertEquals("PUBLISHED", finalAdditionalDistribution.get("status").asText(),
						"added Distribution must be PUBLISHED"),
				() -> assertEquals("Edited Complete Data Distribution", finalDistribution.get("title").get(0).asText(),
						"modified Distribution title must survive publication"),
				() -> assertEquals(0, finalDistribution.get("accessURL").size(),
						"cleared Distribution accessURL must survive publication"),
				() -> assertEquals("Edited distribution description",
						finalDistribution.get("description").get(0).asText(),
						"modified Distribution description must survive publication"));
		assertRelationContainsInstance(finalDistribution, "accessService", draftWebService);
		assertRelationContainsInstance(finalDistribution, "supportedOperation", draftOperation);
		assertRelationContainsInstance(finalDistribution, "dataProduct", finalDataProduct);

		JsonNode finalWebService = retrieveEntity("/webservice", finalDistribution.get("accessService").get(0),
				admin.getAuthIdentifier());
		JsonNode finalOperation = retrieveEntity("/operation", finalDistribution.get("supportedOperation").get(0),
				admin.getAuthIdentifier());
		assertAll("Final WebService and Operation graph",
				() -> assertEquals("PUBLISHED", finalWebService.get("status").asText(),
						"final WebService must be PUBLISHED"),
				() -> assertEquals("Edited Complete Data API", finalWebService.get("name").asText(),
						"modified WebService name must survive publication"),
				() -> assertEquals("PUBLISHED", finalOperation.get("status").asText(),
						"final Operation must be PUBLISHED"),
				() -> assertEquals("POST", finalOperation.get("method").asText(),
						"modified Operation method must survive publication"));
		assertRelationContainsInstance(finalWebService, "supportedOperation", finalOperation);
		assertRelationContainsInstance(finalWebService, "distribution", finalDistribution);
		assertRelationContainsInstance(finalOperation, "webservice", finalWebService);

		assertArchivedEntityPreservesContent("/dataproduct", publishedBaseline.get("dataProduct"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/distribution", publishedBaseline.get("distribution"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/webservice", publishedBaseline.get("webService"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/operation", publishedBaseline.get("operation"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/identifier", publishedBaseline.get("identifier"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/location", publishedBaseline.get("location"),
				admin.getAuthIdentifier());
		assertArchivedEntityPreservesContent("/periodoftime", publishedBaseline.get("periodOfTime"),
				admin.getAuthIdentifier());
	}

	private JsonNode postMetadata(String endpoint, ObjectNode payload, String userId) throws Exception {
		MvcResult result = mockMvc.perform(post(endpoint)
				.queryParam("userId", userId)
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(payload)))
				.andExpect(status().isCreated())
				.andReturn();
		return objectMapper.readTree(result.getResponse().getContentAsString());
	}

	private JsonNode retrieveEntity(String endpoint, String metaId, String instanceId, String userId) throws Exception {
		MvcResult result = mockMvc.perform(get(endpoint + "/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", userId))
				.andExpect(status().isOk())
				.andReturn();
		return objectMapper.readTree(result.getResponse().getContentAsString()).get(0);
	}

	private JsonNode retrieveEntity(String endpoint, JsonNode linkedEntity, String userId) throws Exception {
		return retrieveEntity(endpoint, linkedEntity.get("metaId").asText(),
				linkedEntity.get("instanceId").asText(), userId);
	}

	private void assertDraftEntity(
			String endpoint,
			JsonNode draftLink,
			JsonNode publishedEntity,
			String userId,
			String groupId) throws Exception {
		JsonNode draftEntity = retrieveEntity(endpoint, draftLink, userId);
		assertAll("Child draft " + endpoint,
				() -> assertEquals("DRAFT", draftEntity.get("status").asText(),
						"child draft must be DRAFT"),
				() -> assertEquals(userId, draftEntity.get("editorId").asText(),
						"child draft editor must own the draft"),
				() -> assertEquals(groupId, draftEntity.get("groups").get(0).asText(),
						"child draft must remain in expected group"),
				() -> assertEquals(publishedEntity.get("metaId"), draftEntity.get("metaId"),
						"child draft must preserve metaId"),
				() -> assertEquals(publishedEntity.get("uid"), draftEntity.get("uid"),
						"child draft must preserve uid"),
				() -> assertEquals(publishedEntity.get("instanceId"), draftEntity.get("instanceChangedId"),
						"child draft lineage must point to published child"),
				() -> assertNotEquals(publishedEntity.get("instanceId"), draftEntity.get("instanceId"),
						"child draft must have a new instanceId"));
	}

	private void assertPublishedEntityLink(JsonNode link, JsonNode publishedEntity) {
		assertAll("Published reference link",
				() -> assertEquals(publishedEntity.get("instanceId"), link.get("instanceId"),
						"published reference must preserve instanceId"),
				() -> assertEquals(publishedEntity.get("metaId"), link.get("metaId"),
						"published reference must preserve metaId"),
				() -> assertEquals(publishedEntity.get("uid"), link.get("uid"),
						"published reference must preserve uid"));
	}

	private void assertRelationContainsInstance(JsonNode entity, String relationName, JsonNode expectedLink) {
		assertNotNull(entity.get(relationName), () -> "Missing relation: " + relationName + " in " + entity);
		assertTrue(entity.get(relationName).findValuesAsText("instanceId")
				.contains(expectedLink.get("instanceId").asText()),
				() -> "Missing relation instance " + expectedLink.get("instanceId") + " in " + entity);
	}

	private JsonNode findRelationByUid(JsonNode entity, String relationName, String uid) {
		for (JsonNode relation : entity.get(relationName)) {
			if (uid.equals(relation.get("uid").asText())) {
				return relation;
			}
		}
		return null;
	}

	private void assertArchivedEntityPreservesContent(String endpoint, JsonNode publishedEntity, String userId)
			throws Exception {
		JsonNode archivedEntity = retrieveEntity(endpoint, publishedEntity, userId);
		ObjectNode expected = publishedEntity.deepCopy();
		ObjectNode actual = archivedEntity.deepCopy();
		expected.put("status", "ARCHIVED");
		expected.remove("changeTimestamp");
		expected.remove("changeComment");
		actual.remove("changeTimestamp");
		actual.remove("changeComment");
		assertAll("Check archived " + endpoint,
				() -> assertEquals("ARCHIVED", archivedEntity.get("status").asText(),
						"previous published entity must be ARCHIVED"),
				() -> assertEquals(expected, actual, "archiving must not mutate public entity content"));
	}

	private ObjectNode entityUpdatePayload(JsonNode entity) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.set("instanceId", entity.get("instanceId"));
		payload.set("metaId", entity.get("metaId"));
		payload.set("uid", entity.get("uid"));
		if (entity.has("instanceChangedId")) {
			payload.set("instanceChangedId", entity.get("instanceChangedId"));
		}
		payload.set("status", entity.get("status"));
		payload.set("groups", entity.get("groups"));
		return payload;
	}

	private void assertPublishedGraphUnchanged(ObjectNode publishedBaseline, String userId) throws Exception {
		assertAll("Published graph preservation",
				() -> assertEquals(publishedBaseline.get("dataProduct"),
						retrieveEntity("/dataproduct", publishedBaseline.get("dataProduct"), userId),
						"published DataProduct must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("identifier"),
						retrieveEntity("/identifier", publishedBaseline.get("identifier"), userId),
						"published Identifier must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("category"),
						retrieveEntity("/category", publishedBaseline.get("category"), userId),
						"published Category must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("publisher"),
						retrieveEntity("/organization", publishedBaseline.get("publisher"), userId),
						"published publisher must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("contactPoint"),
						retrieveEntity("/contactpoint", publishedBaseline.get("contactPoint"), userId),
						"published ContactPoint must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("location"),
						retrieveEntity("/location", publishedBaseline.get("location"), userId),
						"published Location must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("periodOfTime"),
						retrieveEntity("/periodoftime", publishedBaseline.get("periodOfTime"), userId),
						"published PeriodOfTime must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("operation"),
						retrieveEntity("/operation", publishedBaseline.get("operation"), userId),
						"published Operation must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("webService"),
						retrieveEntity("/webservice", publishedBaseline.get("webService"), userId),
						"published WebService must remain unchanged"),
				() -> assertEquals(publishedBaseline.get("distribution"),
						retrieveEntity("/distribution", publishedBaseline.get("distribution"), userId),
						"published Distribution must remain unchanged"));
	}

	private void updateMetadata(String endpoint, ObjectNode payload, String userId) throws Exception {
		mockMvc.perform(put(endpoint)
				.queryParam("userId", userId)
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(payload)))
				.andExpect(status().isCreated());
	}

	private void addRelation(ObjectNode dataProduct, String relationName, JsonNode linkedEntity) {
		ArrayNode relations;
		if (dataProduct.get(relationName) instanceof ArrayNode existingRelations) {
			relations = existingRelations;
		} else {
			relations = objectMapper.createArrayNode();
			dataProduct.set(relationName, relations);
		}
		relations.add(linkedEntity(linkedEntity));
	}

	private ObjectNode linkedEntity(JsonNode source) {
		ObjectNode linkedEntity = objectMapper.createObjectNode();
		linkedEntity.put("entityType", source.get("entityType").asText());
		linkedEntity.put("instanceId", source.get("instanceId").asText());
		linkedEntity.put("metaId", source.get("metaId").asText());
		linkedEntity.put("uid", source.get("uid").asText());
		return linkedEntity;
	}

	private void assertDataProductRelation(
			String metaId,
			String instanceId,
			String userId,
			String relationName,
			JsonNode linkedEntity) throws Exception {
		mockMvc.perform(get("/dataproduct/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", userId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].status").value("DRAFT"))
				.andExpect(jsonPath("$[0]." + relationName + "[*].entityType",
						hasItem(linkedEntity.get("entityType").asText())))
				.andExpect(jsonPath("$[0]." + relationName + "[*].instanceId",
						hasItem(linkedEntity.get("instanceId").asText())))
				.andExpect(jsonPath("$[0]." + relationName + "[*].metaId",
						hasItem(linkedEntity.get("metaId").asText())))
				.andExpect(jsonPath("$[0]." + relationName + "[*].uid",
						hasItem(linkedEntity.get("uid").asText())));
	}

	private void assertDataProductRelations(
			String metaId,
			String instanceId,
			String userId,
			JsonNode identifier,
			JsonNode category,
			JsonNode publisher,
			JsonNode contactPoint,
			JsonNode location,
			JsonNode periodOfTime,
			JsonNode distribution) throws Exception {
		assertDataProductRelations(metaId, instanceId, userId, identifier, category, publisher,
				contactPoint, location, periodOfTime, distribution, "DRAFT");
	}

	private void assertDataProductRelations(
			String metaId,
			String instanceId,
			String userId,
			JsonNode identifier,
			JsonNode category,
			JsonNode publisher,
			JsonNode contactPoint,
			JsonNode location,
			JsonNode periodOfTime,
			JsonNode distribution,
			String expectedStatus) throws Exception {
		JsonNode dataProduct = retrieveEntity("/dataproduct", metaId, instanceId, userId);
		assertAll("DataProduct relation graph " + expectedStatus,
				() -> assertEquals(expectedStatus, dataProduct.get("status").asText(),
						"DataProduct status must be " + expectedStatus),
				() -> assertRelationContainsInstance(dataProduct, "identifier", identifier),
				() -> assertRelationContainsInstance(dataProduct, "category", category),
				() -> assertRelationContainsInstance(dataProduct, "publisher", publisher),
				() -> assertRelationContainsInstance(dataProduct, "contactPoint", contactPoint),
				() -> assertRelationContainsInstance(dataProduct, "spatialExtent", location),
				() -> assertRelationContainsInstance(dataProduct, "temporalExtent", periodOfTime),
				() -> assertRelationContainsInstance(dataProduct, "distribution", distribution));
	}

	private void assertDataProductFields(
			String metaId,
			String instanceId,
			String userId,
			String uid,
			String groupId) throws Exception {
		assertDataProductFields(metaId, instanceId, userId, uid, groupId, "DRAFT");
	}

	private void assertDataProductFields(
			String metaId,
			String instanceId,
			String userId,
			String uid,
			String groupId,
			String expectedStatus) throws Exception {
		mockMvc.perform(get("/dataproduct/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", userId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].instanceId").value(instanceId))
				.andExpect(jsonPath("$[0].metaId").value(metaId))
				.andExpect(jsonPath("$[0].uid").value(uid))
				.andExpect(jsonPath("$[0].status").value(expectedStatus))
				.andExpect(jsonPath("$[0].editorId").value(userId))
				.andExpect(jsonPath("$[0].groups", containsInAnyOrder(groupId)))
				.andExpect(jsonPath("$[0].fileProvenance").value("backoffice"))
				.andExpect(jsonPath("$[0].title[0]").value("Complete DataProduct"))
				.andExpect(jsonPath("$[0].description[0]").value("Complete DataProduct description"))
				.andExpect(jsonPath("$[0].keywords").value("seismology,earthquakes"))
				.andExpect(jsonPath("$[0].type").value("http://purl.org/dc/dcmitype/Collection"))
				.andExpect(jsonPath("$[0].accrualPeriodicity").value("daily"))
				.andExpect(jsonPath("$[0].accessRight").value("open data"))
				.andExpect(jsonPath("$[0].documentation").value("https://example.org/documentation"))
				.andExpect(jsonPath("$[0].qualityAssurance").value("https://example.org/quality"))
				.andExpect(jsonPath("$[0].created").value(startsWith("2024-01-02")))
				.andExpect(jsonPath("$[0].issued").value(startsWith("2024-01-03")))
				.andExpect(jsonPath("$[0].modified").value(startsWith("2024-01-04")))
				.andExpect(jsonPath("$[0].versionInfo").value("1.0"))
				.andExpect(jsonPath("$[0].provenance[0]").value("https://example.org/provenance"))
				.andExpect(jsonPath("$[0].landingPage[0]").value("https://example.org/data"))
				.andExpect(jsonPath("$[0].referencedBy[0]").value("https://example.org/reference"))
				.andExpect(jsonPath("$[0].variableMeasured[0]").value("ground motion"));
		mockMvc.perform(get("/dataproduct/{metaId}", metaId)
				.queryParam("userId", userId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("[*].instanceId", hasItem(instanceId)))
				.andExpect(jsonPath("[*].metaId", hasItem(metaId)))
				.andExpect(jsonPath("[*].uid", hasItem(uid)))
				.andExpect(jsonPath("$", hasSize(1)));
	}

	private void assertEntityStatus(String endpoint, JsonNode linkedEntity, String userId, String expectedStatus)
			throws Exception {
		mockMvc.perform(get(endpoint + "/{metaId}/{instanceId}",
				linkedEntity.get("metaId").asText(), linkedEntity.get("instanceId").asText())
				.queryParam("userId", userId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].status").value(expectedStatus));
	}
}
