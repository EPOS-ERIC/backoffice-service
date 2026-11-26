package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;
import org.epos.backoffice.api.util.MigrationExecution;
import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

@RestController
@RequestMapping(
        value = "/migration",
        produces = {"application/json"}
)
public class MigrationController implements ApiDocTag{

    private static final Logger log = LoggerFactory.getLogger(MigrationController.class);

    protected final ObjectMapper objectMapper;
    private final HttpServletRequest request;

    class LocalDateAdapter implements JsonSerializer<LocalDateTime> {

        public JsonElement serialize(LocalDateTime date, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(date.format(DateTimeFormatter.ISO_DATE_TIME)); // "yyyy-mm-dd"
        }
    }

    protected Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(LocalDateTime.class, new MigrationController.LocalDateAdapter())
            .create();

    protected User getUserFromSession() {
        return (User) request.getSession().getAttribute("user");
    }

    @org.springframework.beans.factory.annotation.Autowired
    public MigrationController(ObjectMapper objectMapper, HttpServletRequest request) {
        this.objectMapper = objectMapper;
        this.request = request;
    }

    /**
     * Gson elements (JsonArray / JsonObject) must not be handed to Spring as the response body:
     * Spring serializes with Jackson, which treats them as plain beans and calls getters such as
     * getAsString(), failing with "Array must have size 1, but has size N".
     * They are therefore serialized here with Gson and returned as a JSON string.
     */
    private ResponseEntity<String> toJsonResponse(JsonElement body) {
        if (body == null) {
            // MigrationExecution returns null when the user is not an admin
            return jsonError(HttpStatus.FORBIDDEN, "Forbidden: admin privileges required.");
        }
        if (body.isJsonObject() && body.getAsJsonObject().isEmpty()) {
            return jsonError(HttpStatus.NOT_FOUND, "Not found.");
        }
        return ResponseEntity.status(HttpStatus.OK)
                .contentType(MediaType.APPLICATION_JSON)
                .body(gson.toJson(body));
    }

    private ResponseEntity<String> jsonError(HttpStatus status, String message) {
        JsonObject error = new JsonObject();
        error.addProperty("error", message);
        return ResponseEntity.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .body(gson.toJson(error));
    }

    ResponseEntity<String> migrationResponse(String instanceId, String type, Supplier<JsonObject> export) {
        MigrationExecution.beginMigrationDebug();
        JsonObject payload;
        JsonArray events;
        try {
            payload = export.get();
        } catch (IllegalArgumentException e) {
            events = MigrationExecution.endMigrationDebug();
            JsonObject debug = new JsonObject();
            debug.addProperty("instanceid", instanceId);
            debug.addProperty("type", type);
            debug.add("events", events);
            debug.addProperty("error", e.getMessage());
            JsonObject response = new JsonObject();
            response.add("debug", debug);
            response.add("payload", JsonNull.INSTANCE);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                    .body(gson.newBuilder().serializeNulls().create().toJson(response));
        } catch (RuntimeException e) {
            events = MigrationExecution.endMigrationDebug();
            log.error("Unexpected migration export error for {} {}", type, instanceId, e);
            JsonObject debug = new JsonObject();
            debug.addProperty("instanceid", instanceId);
            debug.addProperty("type", type);
            debug.add("events", events);
            debug.addProperty("error", "Unexpected error during export");
            JsonObject response = new JsonObject();
            response.add("debug", debug);
            response.add("payload", JsonNull.INSTANCE);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.APPLICATION_JSON)
                    .body(gson.newBuilder().serializeNulls().create().toJson(response));
        }
        events = MigrationExecution.endMigrationDebug();
        if (payload == null || payload.isEmpty()) return toJsonResponse(payload);
        JsonObject debug = new JsonObject();
        debug.addProperty("instanceid", instanceId);
        debug.addProperty("type", type);
        debug.add("events", events);
        JsonObject response = new JsonObject();
        response.add("debug", debug);
        response.add("payload", payload);
        return toJsonResponse(response);
    }

    @RequestMapping(produces = {"application/json"},
            method = RequestMethod.GET)
    @ResponseBody
    @Operation(summary = "Get List of Objects", description = "**** MIGRATION USE ONLY")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The List of objects are correctly retrieved", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "403", description = "User is not an admin"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<String> get() {
        User user = getUserFromSession();
        if (user == null) {
            return jsonError(HttpStatus.UNAUTHORIZED, "Unauthorized: User not found in session.");
        }

        return toJsonResponse(MigrationExecution.getListOfObjects(user));
    }

    @RequestMapping(
            value = "/dataset/{instance_id}",
            produces = {"application/json"},
            method = RequestMethod.GET)
    @ResponseBody
    @Operation(summary = "Get Dataset", description = "**** MIGRATION USE ONLY")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The Dataset are correctly retrieved", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "403", description = "User is not an admin"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<String> getDataset(
            @PathVariable String instance_id) {
        User user = getUserFromSession();
        if (user == null) {
            return jsonError(HttpStatus.UNAUTHORIZED, "Unauthorized: User not found in session.");
        }

        return migrationResponse(instance_id, "Dataset", () -> MigrationExecution.getDataset(instance_id, user));
    }

    @RequestMapping(
            value = "/softwareapplication/{instance_id}",
            produces = {"application/json"},
            method = RequestMethod.GET)
    @ResponseBody
    @Operation(summary = "Get Software Application", description = "**** MIGRATION USE ONLY")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The Software Application are correctly retrieved", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "403", description = "User is not an admin"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<String> getSoftwareApplication(
            @PathVariable String instance_id) {
        User user = getUserFromSession();
        if (user == null) {
            return jsonError(HttpStatus.UNAUTHORIZED, "Unauthorized: User not found in session.");
        }

        return migrationResponse(instance_id, "SoftwareApplication", () -> MigrationExecution.getSoftwareApplication(instance_id, user));
    }

    @RequestMapping(
            value = "/softwaresourcecode/{instance_id}",
            produces = {"application/json"},
            method = RequestMethod.GET)
    @ResponseBody
    @Operation(summary = "Get Software Source Code", description = "**** MIGRATION USE ONLY")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The Software Source Code are correctly retrieved", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "403", description = "User is not an admin"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<String> getSoftwareSourceCode(
            @PathVariable String instance_id) {
        User user = getUserFromSession();
        if (user == null) {
            return jsonError(HttpStatus.UNAUTHORIZED, "Unauthorized: User not found in session.");
        }

        return migrationResponse(instance_id, "SoftwareSourceCode", () -> MigrationExecution.getSoftwareSourceCode(instance_id, user));
    }
}
