package org.epos.backoffice.api.util;

import org.epos.backoffice.api.controller.ReviewNotificationController;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import usermanagementapis.UserGroupManagementAPI;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

public class EmailWrapper {

    private static final Logger log = LoggerFactory.getLogger(EmailWrapper.class);

    private static final RestTemplate restTemplate = new RestTemplate();

    public static void wrapSubmitted(EPOSDataModelEntity edmentity, User user, String meta_id, String instance_id){
        String subject = generateEmailSubject(edmentity);
        String body = generateEmailBody(edmentity, user, meta_id, instance_id);

        ReviewNotificationController.ReviewEmailRequest externalRequest = new ReviewNotificationController.ReviewEmailRequest(body, subject);

        URI url = UriComponentsBuilder.fromUriString("http://email-sender-service:8080")
                .path("/api/email-sender-service/v1/sender/send-email-group/")
                .pathSegment("Metadata Curators")
                .build()
                .toUri();
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<ReviewNotificationController.ReviewEmailRequest> entity = new HttpEntity<>(externalRequest, headers);

            restTemplate.postForEntity(url, entity, Void.class);
            log.debug("Review request sent successfully");
        } catch (Exception e) {
            log.error("Error calling email-sender-service", e);
        }
    }

    private static String generateEmailSubject(EPOSDataModelEntity edmentity) {
        if(edmentity instanceof DataProduct)
            return String.format("[EPOS] Review Request: %s", ((DataProduct) edmentity).getTitle());
        else
            return String.format("[EPOS] Review Request: %s", edmentity.getUid());

    }

    private static String generateEmailBody(EPOSDataModelEntity edmentity, User submitter, String metaId, String instanceId) {
        String groupList = "N/A";
        if (submitter.getGroups() != null && !submitter.getGroups().isEmpty()) {
            groupList = submitter.getGroups().stream()
                    .filter(group -> group != null && group.getGroupId() != null)
                    .map(group -> UserGroupManagementAPI.retrieveGroupById(group.getGroupId()).getName())
                    .collect(Collectors.joining(", "));
            if (groupList.isBlank()) {
                groupList = "N/A";
            }
        }

        String title = null;

        if(edmentity instanceof DataProduct) {
            if (((DataProduct) edmentity).getTitle() != null) {
                title = String.valueOf(((DataProduct) edmentity).getTitle());
            }
        }

        String className = edmentity.getClass().getSimpleName();

        return String.format(
                "A new draft submission requires your review.\n\n"
                        + className + " Details:\n"
                        + "Title: %s\n"
                        + "UID: %s\n"
                        + "Meta ID: %s\n"
                        + "Instance ID: %s\n\n"
                        + "Submitted by:\n"
                        + "Name: %s %s\n"
                        + "Email: %s\n"
                        + "Groups: %s\n\n"
                        + "Submitted on: %s\n",
                title != null ? title : "N/A",
                edmentity.getUid() != null ? edmentity.getUid() : "N/A",
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
