package org.epos.backoffice.e2e;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

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

import dao.EposDataModelDAO;
import model.OrganizationMemberof;
import model.RequestStatusType;
import model.RoleType;
import usermanagementapis.UserGroupManagementAPI;

class OrganizationDeletionLifecycleTest extends E2eTestInfrastructure {

	@Test
	void deletePublishedOrganizationWithPublishedRelations() throws Exception {
		Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
		assertNotNull(allGroup, "default ALL group must exist");

		User admin = new User(
				"organization-delete-admin",
				"Admin",
				"OrganizationDelete",
				"organization-delete-admin@email.email",
				true);
		assertEquals(ApiResponseMessage.OK, UserManager.createUser(admin, admin).getCode());

		AddUserToGroupBean membership = new AddUserToGroupBean();
		membership.setGroupid(allGroup.getId());
		membership.setUserid(admin.getAuthIdentifier());
		membership.setRole(RoleType.ADMIN.name());
		membership.setStatusType(RequestStatusType.ACCEPTED.name());
		assertEquals(ApiResponseMessage.OK, UserManager.addUserToGroup(membership, admin).getCode());

		JsonNode parent = postMetadata("/organization",
				organizationPayload("ORG-PARENT", "Parent Organization", allGroup.getId()), admin);
		JsonNode member = postMetadata("/organization",
				organizationPayload("ORG-MEMBER", "Member Organization", allGroup.getId()), admin);
		assertPublished("/organization", parent, admin);
		assertPublished("/organization", member, admin);

		JsonNode identifier = postMetadata("/identifier", objectMapper.createObjectNode()
				.put("status", "PUBLISHED")
				.put("type", "PIC")
				.put("identifier", "PIC-ORG-MEMBER"), admin);
		JsonNode address = postMetadata("/address", objectMapper.createObjectNode()
				.put("status", "PUBLISHED")
				.put("country", "Italy")
				.put("countryCode", "IT")
				.put("street", "Via Roma")
				.put("postalCode", "00100")
				.put("locality", "Rome"), admin);
		ObjectNode contactPointPayload = objectMapper.createObjectNode();
		contactPointPayload.put("status", "PUBLISHED");
		contactPointPayload.put("role", "organization manager");
		contactPointPayload.set("email", objectMapper.createArrayNode().add("member@example.org"));
		JsonNode contactPoint = postMetadata("/contactpoint", contactPointPayload, admin);
		assertPublished("/identifier", identifier, admin);
		assertPublished("/address", address, admin);
		assertPublished("/contactpoint", contactPoint, admin);

		ObjectNode memberUpdate = organizationPayload("ORG-MEMBER", "Member Organization", allGroup.getId());
		memberUpdate.set("instanceId", member.get("instanceId"));
		memberUpdate.set("metaId", member.get("metaId"));
		memberUpdate.set("uid", member.get("uid"));
		memberUpdate.set("identifier", objectMapper.createArrayNode().add(linkedEntity(identifier)));
		memberUpdate.set("address", linkedEntity(address));
		memberUpdate.set("contactPoint", objectMapper.createArrayNode().add(linkedEntity(contactPoint)));
		memberUpdate.set("memberOf", objectMapper.createArrayNode().add(linkedEntity(parent)));

		mockMvc.perform(put("/organization")
				.queryParam("userId", admin.getAuthIdentifier())
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(memberUpdate)))
				.andExpect(status().isCreated());

		JsonNode updatedMember = retrieveOrganization(member, admin);
		assertEquals("PUBLISHED", updatedMember.get("status").asText());
		assertRelation(updatedMember, "identifier", identifier);
		assertRelation(updatedMember, "address", address);
		assertRelation(updatedMember, "contactPoint", contactPoint);
		assertRelation(updatedMember, "memberOf", parent);
		assertMemberOfRowReferencesMember(parent, member);

		mockMvc.perform(delete("/organization/{instanceId}", member.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.code").value(ApiResponseMessage.OK))
				.andExpect(jsonPath("$.type").value("ok"));

		mockMvc.perform(get("/organization/{metaId}/{instanceId}", member.get("metaId").asText(),
				member.get("instanceId").asText())
				.queryParam("userId", admin.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$", hasSize(0)));

		assertEquals("PUBLISHED", retrieveOrganization(parent, admin).get("status").asText());
		assertEquals("PUBLISHED", retrieveEntity("/identifier", identifier, admin).get("status").asText());
		assertEquals("PUBLISHED", retrieveEntity("/address", address, admin).get("status").asText());
		assertEquals("PUBLISHED", retrieveEntity("/contactpoint", contactPoint, admin).get("status").asText());
	}

	private ObjectNode organizationPayload(String acronym, String legalName, String groupId) {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("status", "PUBLISHED");
		payload.put("acronym", acronym);
		payload.set("legalName", objectMapper.createArrayNode().add(legalName));
		payload.set("groups", objectMapper.createArrayNode().add(groupId));
		return payload;
	}

	private JsonNode postMetadata(String endpoint, ObjectNode payload, User user) throws Exception {
		MvcResult result = mockMvc.perform(post(endpoint)
				.queryParam("userId", user.getAuthIdentifier())
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(payload)))
				.andExpect(status().isCreated())
				.andReturn();
		return objectMapper.readTree(result.getResponse().getContentAsString());
	}

	private JsonNode retrieveOrganization(JsonNode organization, User user) throws Exception {
		return retrieveEntity("/organization", organization, user);
	}

	private JsonNode retrieveEntity(String endpoint, JsonNode entity, User user) throws Exception {
		MvcResult result = mockMvc.perform(get(endpoint + "/{metaId}/{instanceId}",
				entity.get("metaId").asText(), entity.get("instanceId").asText())
				.queryParam("userId", user.getAuthIdentifier()))
				.andExpect(status().isOk())
				.andReturn();
		return objectMapper.readTree(result.getResponse().getContentAsString()).get(0);
	}

	private void assertPublished(String endpoint, JsonNode entity, User user) throws Exception {
		assertEquals("PUBLISHED", retrieveEntity(endpoint, entity, user).get("status").asText());
	}

	private ObjectNode linkedEntity(JsonNode entity) {
		ObjectNode link = objectMapper.createObjectNode();
		link.put("entityType", entity.get("entityType").asText());
		link.put("instanceId", entity.get("instanceId").asText());
		link.put("metaId", entity.get("metaId").asText());
		link.put("uid", entity.get("uid").asText());
		return link;
	}

	private void assertRelation(JsonNode entity, String relationName, JsonNode expected) {
		assertNotNull(entity.get(relationName), "missing relation " + relationName);
		assertEquals(expected.get("instanceId").asText(),
				entity.get(relationName).findValuesAsText("instanceId").get(0));
	}

	private void assertMemberOfRowReferencesMember(JsonNode parent, JsonNode member) {
		List<OrganizationMemberof> relations = EposDataModelDAO.getInstance().getOneFromDBBySpecificKey(
				"organization2Instance", member.get("instanceId").asText(), OrganizationMemberof.class);

		assertFalse(relations.isEmpty(), "memberOf join row must exist before delete");
		assertEquals(parent.get("instanceId").asText(),
				relations.get(0).getOrganization1Instance().getInstanceId());
	}
}
