package org.epos.backoffice.api.controller;

import java.net.URI;
import java.util.Optional;

import dao.EposDataModelDAO;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.model.StatusType;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.UserGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;
import model.RoleType;

@RestController
@RequestMapping(value = "/populate-ttl", produces = { "*/*" }, consumes = { "text/turtle", "*/*" })
@io.swagger.v3.oas.annotations.tags.Tag(name = "Backoffice Service - Metadata APIs")
public class PopulateTTLController implements ApiDocTag {

	private final RestTemplate restTemplate;
	private final HttpServletRequest request;
	private static final String INGESTOR_SERVICE_BASE_URL = "http://ingestor-service:8080/api/ingestor-service/v1/";

	@Autowired
	public PopulateTTLController(RestTemplate restTemplate, HttpServletRequest request) {
		this.restTemplate = restTemplate;
		this.request = request;
	}

	@PostMapping
	@io.swagger.v3.oas.annotations.Operation(summary = "Populate metadata with DRAFT status", description = "Proxies the metadata population to the ingestor service, forcing the status to DRAFT.")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "ok.", content = @Content(mediaType = "*/*", schema = @Schema(implementation = ApiResponseMessage.class))),
			@ApiResponse(responseCode = "201", description = "Created.", content = @Content(mediaType = "*/*", schema = @Schema(implementation = ApiResponseMessage.class))),
			@ApiResponse(responseCode = "204", description = "No content.", content = @Content(mediaType = "*/*", schema = @Schema(implementation = ApiResponseMessage.class))),
			@ApiResponse(responseCode = "301", description = "Moved Permanently.", content = @Content(mediaType = "*/*", schema = @Schema(implementation = ApiResponseMessage.class))),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "403", description = "Forbidden"),
			@ApiResponse(responseCode = "404", description = "Not Found")
	})
	public ResponseEntity<String> populateTTL(
			@RequestHeader HttpHeaders headers,
			@Parameter(in = ParameterIn.QUERY, description = "population type (single file or multiple lines file)", required = true, schema = @Schema(allowableValues = {
					"single", "multiple" })) @RequestParam(value = "type", required = true) String type,
			@Parameter(in = ParameterIn.QUERY, description = "path of the file to use", required = false, schema = @Schema()) @RequestParam(value = "path", required = false) String path,
			@Parameter(in = ParameterIn.QUERY, description = "metadata model", required = true, schema = @Schema()) @RequestParam(value = "model", required = true) String model,
			@Parameter(in = ParameterIn.QUERY, description = "metadata mapping model", required = true, schema = @Schema()) @RequestParam(value = "mapping", required = true) String mapping,
			@Parameter(in = ParameterIn.QUERY, description = "metadata group where the resource should be placed", required = false, schema = @Schema()) @RequestParam(value = "metadataGroup", required = false) String metadataGroup,
			@RequestBody(required = false) String body) {

		User user = (User) request.getSession().getAttribute("user");
		if (user == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Unauthorized: User not found in session.");
		}

		boolean isAuthorized = false;
		if (user.getIsAdmin()) {
			isAuthorized = true;
		} else if (metadataGroup != null && !metadataGroup.isEmpty()) {
			for (UserGroup group : user.getGroups()) {
				if (group.getGroupId().equals(metadataGroup) &&
						(group.getRole().equals(RoleType.ADMIN) ||
								group.getRole().equals(RoleType.REVIEWER) ||
								group.getRole().equals(RoleType.EDITOR))) {
					isAuthorized = true;
					break;
				}
			}
		}

		if (!isAuthorized) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
					.body("Unauthorized: User does not have sufficient permissions for this metadata group.");
		}

		UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(INGESTOR_SERVICE_BASE_URL + "populate")
				.queryParam("type", type)
				.queryParam("model", model)
				.queryParam("mapping", mapping)
				.queryParam("status", StatusType.DRAFT.name()); // Force status to DRAFT

		Optional.ofNullable(path).ifPresent(p -> uriBuilder.queryParam("path", p));
		Optional.ofNullable(metadataGroup).ifPresent(mg -> uriBuilder.queryParam("metadataGroup", mg));

		URI uri = uriBuilder.build().toUri();

		HttpHeaders forwardedHeaders = new HttpHeaders();
		headers.forEach((key, value) -> {
			if (!key.equalsIgnoreCase(HttpHeaders.HOST) && !key.equalsIgnoreCase(HttpHeaders.CONTENT_LENGTH)) {
				forwardedHeaders.addAll(key, value);
			}
		});
		forwardedHeaders.setContentType(MediaType.TEXT_PLAIN);

		HttpEntity<String> requestEntity = new HttpEntity<>(body != null ? body : "", forwardedHeaders);

		try {
			ResponseEntity<String> result = restTemplate.exchange(uri, HttpMethod.POST, requestEntity, String.class);
			EposDataModelDAO.getInstance().clearAllCaches();
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body("Error forwarding request: " + e.getMessage());
		}
	}
}
