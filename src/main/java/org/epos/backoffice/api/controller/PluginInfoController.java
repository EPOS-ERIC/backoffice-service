package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.epos.eposdatamodel.Distribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/plugin-info", produces = {"application/json"})
public class PluginInfoController extends MetadataAbstractController<Distribution> implements ApiDocTag {

    private static final Logger log = LoggerFactory.getLogger(PluginInfoController.class);

    private final RestTemplate restTemplate;

    public PluginInfoController(ObjectMapper objectMapper, HttpServletRequest request, RestTemplate restTemplate) {
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
        // Check permissions using the same logic as other metadata endpoints
        ResponseEntity<?> authResponse = getMethod(meta_id, instance_id, null);
        if (authResponse.getStatusCodeValue() != 200) {
            return authResponse;
        }

        // If auth succeeds, proxy to converter-service
        String url = "http://converter-service:8080/api/converter-service/v1/distributions/" + instance_id;
        try {
            ResponseEntity<String> proxyResponse = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            return proxyResponse;
        } catch (Exception e) {
            log.error("Error proxying to converter-service: {}", e.getMessage());
            return ResponseEntity.status(500).body("Error retrieving plugin information: " + e.getMessage());
        }
    }
}
