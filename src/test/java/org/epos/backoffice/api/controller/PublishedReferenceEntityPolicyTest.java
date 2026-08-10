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
import org.epos.eposdatamodel.ContactPoint;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PublishedReferenceEntityPolicyTest extends TestcontainersLifecycle {

    @Test
    void editorReadsAdminContactPointFromDraftDataProductWithoutCreatingADraftContactPoint() {
        User admin = new User("reference-admin-" + UUID.randomUUID(), "Admin", "Reference", "admin@example.org", true);
        User editor = new User("reference-editor-" + UUID.randomUUID(), "Editor", "Reference", "editor@example.org", false);
        UserManager.createUser(admin, admin);
        UserManager.createUser(editor, admin);

        Group group = new Group(UUID.randomUUID().toString(), "Reference entities", "Shared reference entities");
        GroupManager.createGroup(group, admin);
        addEditorToGroup(editor, group, admin);

        ContactPoint contactPoint = new ContactPoint();
        contactPoint.setRole("data manager");
        contactPoint.setEmail(List.of("data@example.org"));
        contactPoint.setGroups(List.of(group.getId()));
        LinkedEntity contactPointLink = EPOSDataModelManager.createEposDataModelEntity(
                contactPoint, admin, EntityNames.CONTACTPOINT, ContactPoint.class).getEntity();

        ContactPoint publishedContactPoint = retrieve(contactPointLink, admin, EntityNames.CONTACTPOINT, ContactPoint.class);
        assertEquals(StatusType.PUBLISHED, publishedContactPoint.getStatus());

        DataProduct dataProduct = new DataProduct();
        dataProduct.setGroups(List.of(group.getId()));
        dataProduct.setStatus(StatusType.PUBLISHED);
        dataProduct.setContactPoint(List.of(contactPointLink));
        LinkedEntity publishedDataProductLink = EPOSDataModelManager.createEposDataModelEntity(
                dataProduct, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity();

        DataProduct draftRequest = retrieve(publishedDataProductLink, admin, EntityNames.DATAPRODUCT, DataProduct.class);
        draftRequest.setStatus(StatusType.DRAFT);
        draftRequest.setEditorId(editor.getAuthIdentifier());
        LinkedEntity draftDataProductLink = EPOSDataModelManager.createEposDataModelEntity(
                draftRequest, editor, EntityNames.DATAPRODUCT, DataProduct.class).getEntity();
        DataProduct editorDraft = retrieve(draftDataProductLink, editor, EntityNames.DATAPRODUCT, DataProduct.class);

        assertEquals(contactPointLink.getInstanceId(), editorDraft.getContactPoint().get(0).getInstanceId());
        ContactPoint editorContactPoint = retrieve(contactPointLink, editor, EntityNames.CONTACTPOINT, ContactPoint.class);
        assertEquals("data manager", editorContactPoint.getRole());
        assertEquals("data@example.org", editorContactPoint.getEmail().get(0));

        ContactPoint editorContactPointRequest = new ContactPoint();
        editorContactPointRequest.setGroups(List.of(group.getId()));
        assertEquals(ApiResponseMessage.UNAUTHORIZED, EPOSDataModelManager.createEposDataModelEntity(
                editorContactPointRequest, editor, EntityNames.CONTACTPOINT, ContactPoint.class).getCode());
    }

    private void addEditorToGroup(User editor, Group group, User admin) {
        AddUserToGroupBean membership = new AddUserToGroupBean();
        membership.setUserid(editor.getAuthIdentifier());
        membership.setGroupid(group.getId());
        membership.setRole(RoleType.EDITOR.name());
        membership.setStatusType(RequestStatusType.ACCEPTED.name());
        assertEquals(ApiResponseMessage.OK, UserManager.addUserToGroup(membership, admin).getCode());
    }

    @SuppressWarnings("unchecked")
    private <T> T retrieve(LinkedEntity link, User user, EntityNames entityName, Class<T> entityClass) {
        return (T) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), user, entityName, entityClass).getListOfEntities().get(0);
    }
}
