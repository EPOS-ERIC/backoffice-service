package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MigrationResponseTest {
    @Test
    void separatesPayloadAndDebug() {
        MigrationController controller = new MigrationController(new ObjectMapper(), mock(HttpServletRequest.class));
        var response = controller.migrationResponse("id", "Dataset", () -> {
            JsonObject payload = new JsonObject();
            payload.addProperty("resource_type", "dataset");
            return payload;
        });
        assertEquals(HttpStatus.OK, response.getStatusCode());
        JsonObject body = JsonParser.parseString(response.getBody()).getAsJsonObject();
        assertEquals("dataset", body.getAsJsonObject("payload").get("resource_type").getAsString());
        assertEquals("id", body.getAsJsonObject("debug").get("instanceid").getAsString());
        assertTrue(body.getAsJsonObject("debug").getAsJsonArray("events").isEmpty());
    }

    @Test
    void includesErrorInDebugAndKeepsFailureStatus() {
        MigrationController controller = new MigrationController(new ObjectMapper(), mock(HttpServletRequest.class));
        var response = controller.migrationResponse("id", "Dataset", () -> { throw new IllegalArgumentException("bad mapping"); });
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        JsonObject body = JsonParser.parseString(response.getBody()).getAsJsonObject();
        assertTrue(body.get("payload").isJsonNull());
        assertEquals("bad mapping", body.getAsJsonObject("debug").get("error").getAsString());
    }
}
