package org.epos.backoffice.api.controller;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

import org.epos.eposdatamodel.DataProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import abstractapis.AbstractAPI;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;
import metadataapis.EntityNames;
import model.StatusType;

@RestController
@RequestMapping(value = "/review-notification", produces = { "application/json" })
public class ReviewNotificationController extends MetadataAbstractController<DataProduct> implements ApiDocTag {

	private static final Logger log = LoggerFactory.getLogger(ReviewNotificationController.class);

	private final RestTemplate restTemplate;

	public ReviewNotificationController(ObjectMapper objectMapper, HttpServletRequest request,
			RestTemplate restTemplate) {
		super(objectMapper, request, DataProduct.class);
		this.restTemplate = restTemplate;
	}

	@RequestMapping(value = "/{meta_id}/{instance_id}", method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Request review for a draft DataProduct submission", description = "Sends an email notification to the metadata curators requesting review of a draft submission.")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "Review request sent successfully"),
			@ApiResponse(responseCode = "400", description = "Bad request"),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "DataProduct not found or access denied"),
			@ApiResponse(responseCode = "500", description = "Error sending review request")
	})
	public ResponseEntity<?> requestReview(@PathVariable String meta_id, @PathVariable String instance_id) {
		ResponseEntity<?> authResponse = getMethod(meta_id, instance_id, null);
		if (authResponse.getStatusCode() != HttpStatus.OK) {
			return authResponse;
		}

		org.epos.eposdatamodel.User user = getUserFromSession();
		if (user == null) {
			return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("User session not found");
		}

		DataProduct dataProduct = (DataProduct) AbstractAPI.retrieveAPI(EntityNames.DATAPRODUCT.name()).retrieve(instance_id);

		if (dataProduct == null) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body("DataProduct not found");
		}

		if (!StatusType.SUBMITTED.equals(dataProduct.getStatus())) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("DataProduct must be in submitted state");
		}

		String subject = generateEmailSubject(dataProduct);
		String body = generateEmailBody(dataProduct, user, meta_id, instance_id);

		ReviewEmailRequest externalRequest = new ReviewEmailRequest(body, subject);

        URI url = UriComponentsBuilder.fromUriString("http://email-sender-service:8080")
                .path("/api/email-sender-service/v1/sender/send-email-group/")
                .pathSegment("Metadata Curators")
                .build()
                .toUri();
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<ReviewEmailRequest> entity = new HttpEntity<>(externalRequest, headers);

			restTemplate.postForEntity(url, entity, Void.class);
			return ResponseEntity.ok("Review request sent successfully");
		} catch (Exception e) {
			log.error("Error calling email-sender-service", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error sending review request");
		}
	}

	private String generateEmailSubject(DataProduct dataProduct) {
		return String.format("[EPOS] Review Request: %s", dataProduct.getTitle());
	}

	private String generateEmailBody(DataProduct dataProduct, org.epos.eposdatamodel.User submitter, String metaId, String instanceId) {
		String groupList = "N/A";
		if (submitter.getGroups() != null && !submitter.getGroups().isEmpty()) {
			groupList = submitter.getGroups().stream()
					.filter(group -> group != null && group.getGroupId() != null)
					.map(group -> group.getGroupId())
					.collect(Collectors.joining(", "));
			if (groupList.isBlank()) {
				groupList = "N/A";
			}
		}

		return String.format(
				"A new draft submission requires your review.\n\n"
						+ "DataProduct Details:\n"
						+ "Title: %s\n"
						+ "UID: %s\n"
						+ "Meta ID: %s\n"
						+ "Instance ID: %s\n\n"
						+ "Submitted by:\n"
						+ "Name: %s %s\n"
						+ "Email: %s\n"
						+ "Groups: %s\n\n"
						+ "Submitted on: %s\n",
				dataProduct.getTitle() != null ? dataProduct.getTitle() : "N/A",
				dataProduct.getUid() != null ? dataProduct.getUid() : "N/A",
				metaId,
				instanceId,
				submitter.getFirstName() != null ? submitter.getFirstName() : "N/A",
				submitter.getLastName() != null ? submitter.getLastName() : "N/A",
				submitter.getEmail() != null ? submitter.getEmail() : "N/A",
				groupList,
				LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
	}

	public static class ReviewEmailRequest {
		private String bodyText;
		private String subject;

		public ReviewEmailRequest() {
		}

		public ReviewEmailRequest(String bodyText, String subject) {
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
