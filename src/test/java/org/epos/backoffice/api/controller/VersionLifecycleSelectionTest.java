package org.epos.backoffice.api.controller;

import metadataapis.EntityNames;
import dao.EposDataModelDAO;
import model.StatusType;
import model.Versioningstatus;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.Address;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class VersionLifecycleSelectionTest extends TestcontainersLifecycle {

    private static User admin;

    @BeforeAll
    static void createAdmin() {
        admin = new User("lifecycle-admin", "Lifecycle", "Admin", "lifecycle@example.org", true);
        UserManager.createUser(admin, admin);
    }

    @Test
    void createsPublishedVersion() {
        Address published = createPublished();

        assertEquals(StatusType.PUBLISHED, published.getStatus());
    }

    @Test
    void createsDraftFromPublishedVersion() {
        Address published = createPublished();
        Address draft = createDraftFrom(published);

        assertNotEquals(published.getInstanceId(), draft.getInstanceId());
        assertEquals(published.getMetaId(), draft.getMetaId());
        assertEquals(StatusType.PUBLISHED, retrieve(published).getStatus());
        assertEquals(StatusType.DRAFT, draft.getStatus());
    }

    @Test
    void submitsTheExactDraftInsteadOfPublishedSibling() {
        Address published = createPublished();
        Address draft = createDraftFrom(published);
        draft.setStatus(StatusType.SUBMITTED);

        Address submitted = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draft, admin, EntityNames.ADDRESS, Address.class).getEntity());

        assertEquals(draft.getInstanceId(), submitted.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submitted.getStatus());
        assertEquals(StatusType.PUBLISHED, retrieve(published).getStatus());
        assertPersistedVersions(published.getUid(), published.getInstanceId(), draft.getInstanceId(), StatusType.SUBMITTED);
    }

    @Test
    void publishesTheExactSubmittedDraftAndArchivesPreviousPublished() {
        Address published = createPublished();
        Address draft = createDraftFrom(published);
        draft.setStatus(StatusType.SUBMITTED);
        Address submitted = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draft, admin, EntityNames.ADDRESS, Address.class).getEntity());
        submitted.setStatus(StatusType.PUBLISHED);

        Address republished = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                submitted, admin, EntityNames.ADDRESS, Address.class).getEntity());

        assertEquals(submitted.getInstanceId(), republished.getInstanceId());
        assertEquals(StatusType.PUBLISHED, republished.getStatus());
        assertEquals(StatusType.ARCHIVED, retrieve(published).getStatus());
        assertPersistedVersions(published.getUid(), published.getInstanceId(), submitted.getInstanceId(), StatusType.PUBLISHED);
    }

    @Test
    void archivesTheExactPublishedVersion() {
        Address published = createPublished();
        published.setStatus(StatusType.ARCHIVED);

        Address archived = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                published, admin, EntityNames.ADDRESS, Address.class).getEntity());

        assertEquals(published.getInstanceId(), archived.getInstanceId());
        assertEquals(StatusType.ARCHIVED, archived.getStatus());
    }

    private Address createPublished() {
        Address address = new Address();
        address.setUid("address/lifecycle/" + UUID.randomUUID());
        address.setCountry("Italy");
        address.setCountryCode("IT");
        address.setLocality("Rome");
        address.setStatus(StatusType.PUBLISHED);
        LinkedEntity link = EPOSDataModelManager.createEposDataModelEntity(
                address, admin, EntityNames.ADDRESS, Address.class).getEntity();
        return retrieve(link);
    }

    private Address createDraftFrom(Address published) {
        Address draftRequest = retrieve(published);
        draftRequest.setStatus(StatusType.DRAFT);
        return retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draftRequest, admin, EntityNames.ADDRESS, Address.class).getEntity());
    }

    @SuppressWarnings("unchecked")
    private Address retrieve(LinkedEntity link) {
        return (Address) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.ADDRESS, Address.class)
                .getListOfEntities().get(0);
    }

    private Address retrieve(Address address) {
        return retrieve(new LinkedEntity().metaId(address.getMetaId()).instanceId(address.getInstanceId()));
    }

    private void assertPersistedVersions(String uid, String previousPublishedInstanceId,
                                         String transitionedInstanceId, StatusType transitionedStatus) {
        List<Versioningstatus> versions = EposDataModelDAO.getInstance()
                .getOneFromDBByUIDNoCache(uid, Versioningstatus.class);

        assertEquals(2, versions.size());
        assertEquals(1, versions.stream()
                .filter(version -> transitionedInstanceId.equals(version.getInstanceId()))
                .filter(version -> transitionedStatus.name().equals(version.getStatus()))
                .count());
        assertEquals(1, versions.stream()
                .filter(version -> previousPublishedInstanceId.equals(version.getInstanceId()))
                .count());
    }
}
