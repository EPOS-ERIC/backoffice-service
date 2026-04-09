package org.epos.backoffice.api.controller;

import metadataapis.EntityNames;
import model.RequestStatusType;
import model.RoleType;
import model.StatusType;
import org.epos.backoffice.api.util.AddUserToGroupBean;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.backoffice.api.util.GroupManager;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.Address;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EntityDraftAndReviewerRulesTest extends TestcontainersLifecycle {

    static User adminUser;
    static User editorUser;
    static User reviewerUser;
    static User secondEditorUser;
    static Group testGroup;

    @Test
    @Order(1)
    public void setupUsersAndGroup() {
        adminUser = new User("draft-rules-admin", "Admin", "DraftRules", "draft-rules-admin@email.email", true);
        UserManager.createUser(adminUser, adminUser);

        editorUser = new User("draft-rules-editor", "Editor", "DraftRules", "draft-rules-editor@email.email", false);
        reviewerUser = new User("draft-rules-reviewer", "Reviewer", "DraftRules", "draft-rules-reviewer@email.email", false);
        secondEditorUser = new User("draft-rules-editor-2", "EditorTwo", "DraftRules", "draft-rules-editor-2@email.email", false);

        UserManager.createUser(editorUser, adminUser);
        UserManager.createUser(reviewerUser, adminUser);
        UserManager.createUser(secondEditorUser, adminUser);

        testGroup = new Group(UUID.randomUUID().toString(), "Draft Rules Group", "Group for draft and reviewer rules tests");
        GroupManager.createGroup(testGroup, adminUser);

        addUserToGroup(editorUser.getAuthIdentifier(), RoleType.EDITOR);
        addUserToGroup(reviewerUser.getAuthIdentifier(), RoleType.REVIEWER);
        addUserToGroup(secondEditorUser.getAuthIdentifier(), RoleType.EDITOR);

        Group retrieved = GroupManager.getGroup(testGroup.getId(), adminUser, false).getListOfGroups().get(0);
        assertNotNull(retrieved);
        assertEquals(testGroup.getId(), retrieved.getId());
        assertEquals(3, retrieved.getUsers().size());
    }

    @Test
    @Order(2)
    public void reviewerCanPublishSubmittedEntityOfAnotherUserInSameGroup() {
        Address address = buildAddress(testGroup.getId());
        ApiResponseMessage createResponse = EPOSDataModelManager.createEposDataModelEntity(
                address, editorUser, EntityNames.ADDRESS, Address.class);

        assertEquals(ApiResponseMessage.OK, createResponse.getCode());
        LinkedEntity created = createResponse.getEntity();
        assertNotNull(created);

        Address editorDraft = (Address) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                created.getMetaId(), created.getInstanceId(), editorUser, EntityNames.ADDRESS, Address.class)
                .getListOfEntities().get(0);

        editorDraft.setStatus(StatusType.SUBMITTED);
        ApiResponseMessage submitResponse = EPOSDataModelManager.updateEposDataModelEntity(
                editorDraft, editorUser, EntityNames.ADDRESS, Address.class);
        assertEquals(ApiResponseMessage.OK, submitResponse.getCode());

        Address reviewerView = (Address) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                created.getMetaId(), created.getInstanceId(), reviewerUser, EntityNames.ADDRESS, Address.class)
                .getListOfEntities().get(0);

        reviewerView.setStatus(StatusType.PUBLISHED);
        ApiResponseMessage publishResponse = EPOSDataModelManager.updateEposDataModelEntity(
                reviewerView, reviewerUser, EntityNames.ADDRESS, Address.class);
        assertEquals(ApiResponseMessage.OK, publishResponse.getCode());

        Address published = (Address) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                created.getMetaId(), created.getInstanceId(), adminUser, EntityNames.ADDRESS, Address.class)
                .getListOfEntities().get(0);
        assertEquals(StatusType.PUBLISHED, published.getStatus());
    }

    @Test
    @Order(3)
    public void userCanHaveOnlyOneDraftPerEntityButAnotherUserCanHaveOwnDraft() {
        Address baseDraft = buildAddress(testGroup.getId());
        ApiResponseMessage baseCreateResponse = EPOSDataModelManager.createEposDataModelEntity(
                baseDraft, editorUser, EntityNames.ADDRESS, Address.class);

        assertEquals(ApiResponseMessage.OK, baseCreateResponse.getCode());
        LinkedEntity baseLinked = baseCreateResponse.getEntity();
        assertNotNull(baseLinked);

        Address firstDraftForSecondEditor = new Address();
        firstDraftForSecondEditor.setInstanceId(baseLinked.getInstanceId());
        firstDraftForSecondEditor.setStatus(StatusType.DRAFT);

        ApiResponseMessage firstSecondEditorDraftResponse = EPOSDataModelManager.updateEposDataModelEntity(
                firstDraftForSecondEditor, secondEditorUser, EntityNames.ADDRESS, Address.class);

        assertEquals(ApiResponseMessage.OK, firstSecondEditorDraftResponse.getCode());
        assertNotNull(firstSecondEditorDraftResponse.getEntity());

        Address duplicateDraftForSecondEditor = new Address();
        duplicateDraftForSecondEditor.setInstanceId(baseLinked.getInstanceId());
        duplicateDraftForSecondEditor.setStatus(StatusType.DRAFT);

        ApiResponseMessage duplicateResponse = EPOSDataModelManager.updateEposDataModelEntity(
                duplicateDraftForSecondEditor, secondEditorUser, EntityNames.ADDRESS, Address.class);

        assertEquals(ApiResponseMessage.OK, duplicateResponse.getCode());
        assertNotNull(duplicateResponse.getEntity());
        assertEquals(
                firstSecondEditorDraftResponse.getEntity().getInstanceId(),
                duplicateResponse.getEntity().getInstanceId()
        );

    }

    private static Address buildAddress(String groupId) {
        Address address = new Address();
        address.setInstanceId(UUID.randomUUID().toString());
        address.setMetaId(UUID.randomUUID().toString());
        address.setUid(UUID.randomUUID().toString());
        address.setCountry("Italy");
        address.setCountryCode("IT");
        address.setStreet("Via Roma");
        address.setPostalCode("00100");
        address.setLocality("Rome");
        address.setStatus(StatusType.DRAFT);
        address.setGroups(List.of(groupId));
        return address;
    }

    private static void addUserToGroup(String userId, RoleType role) {
        AddUserToGroupBean addUserToGroupBean = new AddUserToGroupBean();
        addUserToGroupBean.setGroupid(testGroup.getId());
        addUserToGroupBean.setUserid(userId);
        addUserToGroupBean.setRole(role.toString());
        addUserToGroupBean.setStatusType(RequestStatusType.ACCEPTED.toString());
        UserManager.addUserToGroup(addUserToGroupBean, adminUser);
    }
}
