package org.epos.backoffice.api.util;

import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.Group;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class EmailWrapper {

    private static final Logger log = LoggerFactory.getLogger(EmailWrapper.class);

    private static RestTemplate restTemplate = new RestTemplate();

    public static void wrapSubmitted(EPOSDataModelEntity edmentity, User user, String meta_id, String instance_id){
        String subject = generateEmailSubject(edmentity);
        String body = generateEmailBody(edmentity, user, meta_id, instance_id);

        EmailRequest externalRequest = new EmailRequest(body, subject);

        URI url = UriComponentsBuilder.fromUriString("http://email-sender-service:8080")
                .path("/api/email-sender-service/v1/sender/send-email-group/")
                .pathSegment("Metadata Curators")
                .build()
                .toUri();
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<EmailRequest> entity = new HttpEntity<>(externalRequest, headers);

            restTemplate.postForEntity(url, entity, Void.class);
            log.debug("Review request sent successfully");
        } catch (Exception e) {
            log.error("Error calling email-sender-service", e);
        }
    }

    public static void wrapGroupAccessRequest(AddUserToGroupBean userGroup, User requester) {
        List<String> adminEmails = UserGroupManagementAPI.retrieveAllUsers().stream()
                .filter(Objects::nonNull)
                .filter(admin -> Boolean.TRUE.equals(admin.getIsAdmin()))
                .map(User::getEmail)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(email -> !email.isEmpty())
                .distinct()
                .toList();

        if (adminEmails.isEmpty()) {
            log.warn("Skipping group access request email because no global admin email addresses were found");
            return;
        }

        Group requestedGroup = userGroup.getGroupid() != null
                ? UserGroupManagementAPI.retrieveGroupById(userGroup.getGroupid())
                : null;

        String subject = generateGroupAccessRequestSubject(requestedGroup, userGroup.getGroupid());
        String body = generateGroupAccessRequestBody(userGroup, requester, requestedGroup);

        sendEmailDirect(adminEmails, new EmailRequest(body, subject));
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

    private static String generateGroupAccessRequestSubject(Group requestedGroup, String groupId) {
        String groupName = requestedGroup != null && requestedGroup.getName() != null && !requestedGroup.getName().isBlank()
                ? requestedGroup.getName()
                : groupId != null ? groupId : "N/A";

        return String.format("[EPOS] Group Access Request: %s", groupName);
    }

    private static String generateGroupAccessRequestBody(AddUserToGroupBean userGroup, User requester, Group requestedGroup) {
        String groupName = requestedGroup != null && requestedGroup.getName() != null
                ? requestedGroup.getName()
                : "N/A";

        return String.format(
                "A user requested access to a group that requires approval.\n\n"
                        + "Group Details:\n"
                        + "Name: %s\n"
                        + "ID: %s\n"
                        + "Requested role: %s\n"
                        + "Request status: %s\n\n"
                        + "Requested by:\n"
                        + "Name: %s %s\n"
                        + "Email: %s\n"
                        + "Auth identifier: %s\n"
                        + "Requested on: %s\n",
                groupName,
                userGroup.getGroupid() != null ? userGroup.getGroupid() : "N/A",
                userGroup.getRole() != null ? userGroup.getRole() : "N/A",
                userGroup.getStatusType() != null ? userGroup.getStatusType() : "N/A",
                requester.getFirstName() != null ? requester.getFirstName() : "N/A",
                requester.getLastName() != null ? requester.getLastName() : "N/A",
                requester.getEmail() != null ? requester.getEmail() : "N/A",
                requester.getAuthIdentifier() != null ? requester.getAuthIdentifier() : "N/A",
                userGroup.getUserid() != null ? userGroup.getUserid() : "N/A",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    private static void sendEmailDirect(List<String> emails, EmailRequest externalRequest) {
        URI url = UriComponentsBuilder.fromUriString("http://email-sender-service:8080")
                .path("/api/email-sender-service/v1/sender/send-email-direct")
                .queryParam("email", emails.toArray())
                .build()
                .encode()
                .toUri();

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<EmailRequest> entity = new HttpEntity<>(externalRequest, headers);

            restTemplate.postForEntity(url, entity, Void.class);
            log.debug("Direct email sent successfully to {} recipients", emails.size());
        } catch (Exception e) {
            log.error("Error calling email-sender-service", e);
        }
    }

    static void setRestTemplate(RestTemplate customRestTemplate) {
        restTemplate = customRestTemplate;
    }

    static void resetRestTemplate() {
        restTemplate = new RestTemplate();
    }

    public static class EmailRequest {
        private String bodyText;
        private String subject;

        public EmailRequest() {
        }

        public EmailRequest(String bodyText, String subject) {
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
