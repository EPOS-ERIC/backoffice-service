package org.epos.backoffice.api.controller;

import java.net.URI;

import org.epos.eposdatamodel.Distribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import abstractapis.AbstractAPI;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;
import metadataapis.EntityNames;

@RestController
@RequestMapping(value = "/distribution-plugin", produces = { "application/json" })
public class DistributionPluginController extends MetadataAbstractController<Distribution> implements PluginApiDocTag {

    private static final Logger log = LoggerFactory.getLogger(DistributionPluginController.class);

    private final RestTemplate restTemplate;

    public DistributionPluginController(ObjectMapper objectMapper, HttpServletRequest request,
            RestTemplate restTemplate) {
        super(objectMapper, request, Distribution.class);
        this.restTemplate = restTemplate;
    }

    @RequestMapping(value = "/{meta_id}/{instance_id}", method = RequestMethod.GET)
    @ResponseBody
    @Operation(summary = "Get plugin information for a Distribution instance", description = "Proxies to the converter service to retrieve plugin information for the specified distribution instance, after verifying user access permissions.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Plugin information retrieved successfully", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<?> getPluginInfo(@PathVariable String meta_id, @PathVariable String instance_id) {
        ResponseEntity<?> authResponse = getMethod(meta_id, instance_id, null);
        if (authResponse.getStatusCode() != HttpStatus.OK) {
            return authResponse;
        }

        String url = "http://converter-service:8080/api/converter-service/v1/distributions/" + instance_id;
        try {
            ResponseEntity<String> proxyResponse = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            return proxyResponse;
        } catch (Exception e) {
            log.error("Error proxying to converter-service: {}", e.getMessage());
            return ResponseEntity.status(500).body("Error retrieving plugin information: " + e.getMessage());
        }
    }

    @RequestMapping(value = "/{meta_id}/{instance_id}", method = RequestMethod.DELETE)
    @ResponseBody
    @Operation(summary = "Delete plugin relations for a Distribution instance", description = "Proxies to the converter service to delete all plugin relations for the specified distribution instance, after verifying user access permissions.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Plugin relations deleted successfully", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<?> deletePluginRelations(@PathVariable String meta_id, @PathVariable String instance_id) {
        ResponseEntity<?> authResponse = getMethod(meta_id, instance_id, null);
        if (authResponse.getStatusCode() != HttpStatus.OK) {
            return authResponse;
        }

        String url = "http://converter-service:8080/api/converter-service/v1/plugin-relations/distribution/"
                + instance_id;
        try {
            ResponseEntity<String> proxyResponse = restTemplate.exchange(url, HttpMethod.DELETE, null, String.class);
            return proxyResponse;
        } catch (Exception e) {
            log.error("Error proxying to converter-service: {}", e.getMessage());
            return ResponseEntity.status(500).body("Error deleting plugin relations: " + e.getMessage());
        }
    }

    @RequestMapping(value = "/{meta_id}/{instance_id}/send-email", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    @Operation(summary = "Send email for distribution plugins", description = "Sends an email notification via the email-sender-service including user message and distribution details.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Email sent successfully"),
            @ApiResponse(responseCode = "400", description = "Bad request"),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "404", description = "Distribution not found or access denied"),
            @ApiResponse(responseCode = "500", description = "Error sending email")
    })
    public ResponseEntity<?> sendEmailPlugins(
            @PathVariable String meta_id,
            @PathVariable String instance_id,
            @RequestBody EmailPluginRequest body) {
        ResponseEntity<?> authResponse = getMethod(meta_id, instance_id, null);
        if (authResponse.getStatusCode() != HttpStatus.OK) {
            return authResponse;
        }

        org.epos.eposdatamodel.User user = getUserFromSession();
        if (user == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("User session not found");
        }

        Distribution distribution = (Distribution) AbstractAPI.retrieveAPI(EntityNames.DISTRIBUTION.name())
                .retrieve(instance_id);

        String subject = "[EPOS] Notification for Distribution " + instance_id;
        StringBuilder bodyBuilder = new StringBuilder();
        bodyBuilder.append("This email was sent from the EPOS Backoffice Service on behalf of:\n");
        bodyBuilder.append("Name: ").append(user.getFirstName()).append(" ").append(user.getLastName()).append("\n");
        bodyBuilder.append("Email: ").append(user.getEmail()).append("\n\n");
        bodyBuilder.append("Message:\n");
        bodyBuilder.append("\"").append(body.getMessage()).append("\"\n\n");
        bodyBuilder.append("Distribution Details:\n");
        bodyBuilder.append("- UID: ").append(distribution.getUid()).append("\n");
        bodyBuilder.append("- Meta ID: ").append(meta_id).append("\n");
        bodyBuilder.append("- Instance ID: ").append(instance_id).append("\n");
        bodyBuilder.append("- Title: ").append(distribution.getTitle()).append("\n");

        ExternalEmailRequest externalRequest = new ExternalEmailRequest(bodyBuilder.toString(), subject);

        URI url = UriComponentsBuilder.fromUriString("http://email-sender-service:8080")
                .path("/api/email-sender-service/v1/sender/send-email-group/")
                .pathSegment("Plugin Managers")
                .build()
                .toUri();
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<ExternalEmailRequest> entity = new HttpEntity<>(externalRequest, headers);

            restTemplate.postForEntity(url, entity, Void.class);
            return ResponseEntity.ok("Email sent successfully");
        } catch (Exception e) {
            log.error("Error calling email-sender-service: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error sending email: " + e.getMessage());
        }
    }

    public static class EmailPluginRequest {
        private String message;

        public EmailPluginRequest() {
        }

        public EmailPluginRequest(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    public static class ExternalEmailRequest {
        private String bodyText;
        private String subject;

        public ExternalEmailRequest() {
        }

        public ExternalEmailRequest(String bodyText, String subject) {
            this.bodyText = bodyText;
            this.subject = subject;
        }

        public String getBodyText() {
            return bodyText;
        }

        public void setBodyText(String bodyText) {
            this.bodyText = bodyText;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }
    }
}
