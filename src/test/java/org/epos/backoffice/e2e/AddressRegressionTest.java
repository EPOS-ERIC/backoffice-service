package org.epos.backoffice.e2e;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.epos.backoffice.api.util.AddUserToGroupBean;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import model.RequestStatusType;
import model.RoleType;
import usermanagementapis.UserGroupManagementAPI;

class AddressRegressionTest extends E2eTestInfrastructure {

	@Test
	void addressIsPublishedEditedBySuperAdminAndDeleted() throws Exception {
		Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
		assertNotNull(allGroup, "default ALL group must exist");

		User admin = new User(
				"address-delete-admin",
				"Admin",
				"AddressDelete",
				"address-delete-admin@email.email",
				true);
		ApiResponseMessage adminCreate = UserManager.createUser(admin, admin);
		assertEquals(ApiResponseMessage.OK, adminCreate.getCode(), "admin creation must succeed");

		AddUserToGroupBean membership = new AddUserToGroupBean();
		membership.setGroupid(allGroup.getId());
		membership.setUserid(admin.getAuthIdentifier());
		membership.setRole(RoleType.ADMIN.name());
		membership.setStatusType(RequestStatusType.ACCEPTED.name());
		ApiResponseMessage membershipResponse = UserManager.addUserToGroup(membership, admin);
		assertEquals(ApiResponseMessage.OK, membershipResponse.getCode(), "admin membership must succeed");

		ObjectNode addressPayload = objectMapper.createObjectNode();
		addressPayload.put("status", "PUBLISHED");
		addressPayload.put("country", "Italy");
		addressPayload.put("countryCode", "IT");
		addressPayload.put("street", "Via Roma");
		addressPayload.put("postalCode", "00100");
		addressPayload.put("locality", "Rome");
		addressPayload.set("groups", objectMapper.createArrayNode().add(allGroup.getId()));

		MvcResult createResult = mockMvc.perform(post("/address")
				.queryParam("userId", admin.getAuthIdentifier())
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(addressPayload)))
				.andExpect(status().isCreated())
				.andReturn();
		JsonNode createdAddress = objectMapper.readTree(createResult.getResponse().getContentAsString());
		String metaId = createdAddress.get("metaId").asText();
		String instanceId = createdAddress.get("instanceId").asText();

		mockMvc.perform(get("/address/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$[0].instanceId").value(instanceId))
				.andExpect(jsonPath("$[0].metaId").value(metaId))
				.andExpect(jsonPath("$[0].uid").value(createdAddress.get("uid").asText()))
				.andExpect(jsonPath("$[0].status").value("PUBLISHED"))
				.andExpect(jsonPath("$[0].groups[0]").value(allGroup.getId()))
				.andExpect(jsonPath("$[0].country").value("Italy"))
				.andExpect(jsonPath("$[0].street").value("Via Roma"));

		ObjectNode updatePayload = addressPayload.deepCopy();
		updatePayload.put("instanceId", instanceId);
		updatePayload.put("metaId", metaId);
		updatePayload.put("uid", createdAddress.get("uid").asText());
		updatePayload.put("country", "France");
		updatePayload.put("countryCode", "FR");
		updatePayload.put("street", "Rue de la Paix");
		updatePayload.put("postalCode", "75002");
		updatePayload.put("locality", "Paris");

		MvcResult updateResult = mockMvc.perform(put("/address")
				.queryParam("userId", admin.getAuthIdentifier())
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(updatePayload)))
				.andExpect(status().isCreated())
				.andReturn();
		JsonNode updatedAddressLink = objectMapper.readTree(updateResult.getResponse().getContentAsString());
		String updatedInstanceId = updatedAddressLink.get("instanceId").asText();

		MvcResult updatedAddressResult = mockMvc.perform(get("/address/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andReturn();
		JsonNode updatedAddress = objectMapper.readTree(updatedAddressResult.getResponse().getContentAsString()).get(0);

		if (!instanceId.equals(updatedInstanceId)) {
			mockMvc.perform(get("/address/{metaId}/{instanceId}", metaId, updatedInstanceId)
					.queryParam("userId", admin.getAuthIdentifier()))
					.andExpect(status().isOk())
					.andExpect(jsonPath("$[0].instanceId").value(updatedInstanceId))
					.andExpect(jsonPath("$[0].status").value("DRAFT"))
					.andExpect(jsonPath("$[0].country").value("France"));

			mockMvc.perform(delete("/address/{instanceId}", updatedInstanceId)
					.queryParam("userId", admin.getAuthIdentifier()))
					.andExpect(status().isOk())
					.andExpect(jsonPath("$.code").value(ApiResponseMessage.OK))
					.andExpect(jsonPath("$.type").value("ok"));

			mockMvc.perform(get("/address/{metaId}/{instanceId}", metaId, updatedInstanceId)
					.queryParam("userId", admin.getAuthIdentifier()))
					.andExpect(status().isOk())
					.andExpect(jsonPath("$", hasSize(0)));
		}

		mockMvc.perform(delete("/address/{instanceId}", instanceId)
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.code").value(ApiResponseMessage.OK))
				.andExpect(jsonPath("$.type").value("ok"));

		mockMvc.perform(get("/address/{metaId}/{instanceId}", metaId, instanceId)
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$", hasSize(0)));

		assertAll("published Address update",
				() -> assertEquals(instanceId, updatedInstanceId,
						"super-admin update must preserve Address instanceId"),
				() -> assertEquals("PUBLISHED", updatedAddress.get("status").asText(),
						"Address must remain PUBLISHED after update"),
				() -> assertEquals("France", updatedAddress.get("country").asText()),
				() -> assertEquals("Rue de la Paix", updatedAddress.get("street").asText()),
				() -> assertEquals("75002", updatedAddress.get("postalCode").asText()),
				() -> assertEquals("Paris", updatedAddress.get("locality").asText()));
	}
}
