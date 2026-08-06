package org.epos.backoffice.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class EditorIdOwnershipAuthorizationTest extends E2eTestInfrastructure {

    @Test
    void onlyAdminsCanAssignAnEditorDifferentFromTheSessionUser() throws Exception {
        String suffix = UUID.randomUUID().toString();
        User admin = new User("editor-id-admin-" + suffix, "Admin", "EditorId",
                "editor-id-admin-" + suffix + "@email.email", true);
        User editor = new User("editor-id-editor-" + suffix, "Editor", "EditorId",
                "editor-id-editor-" + suffix + "@email.email", false);
        assertEquals(ApiResponseMessage.OK, UserManager.createUser(admin, admin).getCode());
        assertEquals(ApiResponseMessage.OK, UserManager.createUser(editor, admin).getCode());

        String requestedEditorId = "editor-id-owner-" + suffix;
        ObjectNode foreignOwnerPayload = addressPayload(requestedEditorId);

        mockMvc.perform(post("/address")
                .queryParam("userId", editor.getAuthIdentifier())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(foreignOwnerPayload)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.response").value("Only an admin can set a different editorId"));

        MvcResult createResult = mockMvc.perform(post("/address")
                .queryParam("userId", admin.getAuthIdentifier())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(foreignOwnerPayload)))
                .andExpect(status().isCreated())
                .andReturn();
        JsonNode created = objectMapper.readTree(createResult.getResponse().getContentAsString());

        mockMvc.perform(get("/address/{metaId}/{instanceId}", created.get("metaId").asText(),
                created.get("instanceId").asText())
                .queryParam("userId", admin.getAuthIdentifier()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].editorId").value(requestedEditorId));

        ObjectNode updatePayload = addressPayload("another-owner-" + suffix);
        updatePayload.put("instanceId", created.get("instanceId").asText());
        updatePayload.put("metaId", created.get("metaId").asText());
        updatePayload.put("uid", created.get("uid").asText());
        updatePayload.put("status", "DRAFT");

        mockMvc.perform(put("/address")
                .queryParam("userId", editor.getAuthIdentifier())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(updatePayload)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.response").value("Only an admin can set a different editorId"));

        MvcResult updateResult = mockMvc.perform(put("/address")
                .queryParam("userId", admin.getAuthIdentifier())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(updatePayload)))
                .andExpect(status().isCreated())
                .andReturn();
        JsonNode updated = objectMapper.readTree(updateResult.getResponse().getContentAsString());

        mockMvc.perform(get("/address/{metaId}/{instanceId}", updated.get("metaId").asText(),
                updated.get("instanceId").asText())
                .queryParam("userId", admin.getAuthIdentifier()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].editorId").value("another-owner-" + suffix));
    }

    private ObjectNode addressPayload(String editorId) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("country", "Italy");
        payload.put("countryCode", "IT");
        payload.put("street", "Via Roma");
        payload.put("postalCode", "00100");
        payload.put("locality", "Rome");
        payload.put("editorId", editorId);
        return payload;
    }
}
