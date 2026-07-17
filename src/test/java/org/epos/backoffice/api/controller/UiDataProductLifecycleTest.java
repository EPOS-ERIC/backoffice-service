package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.threetenbp.ThreeTenModule;
import dao.EposDataModelDAO;
import metadataapis.EntityNames;
import model.StatusType;
import model.Versioningstatus;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.Distribution;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.Mapping;
import org.epos.eposdatamodel.Location;
import org.epos.eposdatamodel.Operation;
import org.epos.eposdatamodel.PeriodOfTime;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.WebService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UiDataProductLifecycleTest extends TestcontainersLifecycle {

    private static User admin;

    @BeforeAll
    static void createAdmin() {
        admin = new User("ui-lifecycle-admin", "UI", "Lifecycle", "ui-lifecycle@example.org", true);
        UserManager.createUser(admin, admin);
    }

    @Test
    void transitionsTheUiDataProductWithoutCreatingASubmittedVersion() throws Exception {
        DataProduct payload = readUiPayload();
        payload.setStatus(StatusType.PUBLISHED);
        DataProduct published = retrieve(EPOSDataModelManager.createEposDataModelEntity(
                payload, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        DataProduct draftRequest = retrieve(published);
        draftRequest.setStatus(StatusType.DRAFT);
        DataProduct draft = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draftRequest, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        assertNotEquals(published.getInstanceId(), draft.getInstanceId());
        assertEquals(StatusType.PUBLISHED, retrieve(published).getStatus());
        assertEquals(StatusType.DRAFT, draft.getStatus());

        draft.setStatus(StatusType.SUBMITTED);
        DataProduct submitted = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draft, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        assertEquals(draft.getInstanceId(), submitted.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submitted.getStatus());
        assertVersions(published.getUid(), published.getInstanceId(), draft.getInstanceId(), StatusType.SUBMITTED);

        submitted.setStatus(StatusType.PUBLISHED);
        DataProduct republished = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                submitted, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        assertEquals(draft.getInstanceId(), republished.getInstanceId());
        assertEquals(StatusType.PUBLISHED, republished.getStatus());
        assertEquals(StatusType.ARCHIVED, retrieve(published).getStatus());

        republished.setStatus(StatusType.ARCHIVED);
        DataProduct archived = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                republished, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        assertEquals(republished.getInstanceId(), archived.getInstanceId());
        assertEquals(StatusType.ARCHIVED, archived.getStatus());
    }

    @Test
    void submittingUiDataProductKeepsModifiedDraftDistribution() throws Exception {
        Mapping sourceMapping = new Mapping();
        sourceMapping.setUid("https://example.org/mapping/ui-lifecycle");
        sourceMapping.setStatus(StatusType.PUBLISHED);
        Mapping publishedMapping = retrieveMapping(EPOSDataModelManager.createEposDataModelEntity(
                sourceMapping, admin, EntityNames.MAPPING, Mapping.class).getEntity());

        Operation sourceOperation = new Operation();
        sourceOperation.setUid("https://example.org/operation/ui-lifecycle");
        sourceOperation.setStatus(StatusType.PUBLISHED);
        sourceOperation.setMapping(List.of(new LinkedEntity()
                .entityType(EntityNames.MAPPING.name())
                .instanceId(publishedMapping.getInstanceId())
                .metaId(publishedMapping.getMetaId())
                .uid(publishedMapping.getUid())));
        Operation publishedOperation = retrieveOperation(EPOSDataModelManager.createEposDataModelEntity(
                sourceOperation, admin, EntityNames.OPERATION, Operation.class).getEntity());

        WebService sourceWebService = new WebService();
        sourceWebService.setUid("https://example.org/webservice/ui-lifecycle");
        sourceWebService.setName("UI lifecycle WebService");
        sourceWebService.setStatus(StatusType.PUBLISHED);
        sourceWebService.setSupportedOperation(List.of(new LinkedEntity()
                .entityType(EntityNames.OPERATION.name())
                .instanceId(publishedOperation.getInstanceId())
                .metaId(publishedOperation.getMetaId())
                .uid(publishedOperation.getUid())));
        WebService publishedWebService = retrieveWebService(EPOSDataModelManager.createEposDataModelEntity(
                sourceWebService, admin, EntityNames.WEBSERVICE, WebService.class).getEntity());

        Distribution sourceDistribution = new Distribution();
        sourceDistribution.setUid("https://www.epos-eu.org/epos-dcat-ap/NearFaultObservatory/KOERI/WAVEFORM_CONTINUOUS/Distribution/001");
        sourceDistribution.setTitle(List.of("Original distribution title"));
        sourceDistribution.setDescription(List.of("Original distribution description"));
        sourceDistribution.setFormat("application/json");
        sourceDistribution.setLicence("https://example.org/license");
        sourceDistribution.setAccessService(List.of(new LinkedEntity()
                .entityType(EntityNames.WEBSERVICE.name())
                .instanceId(publishedWebService.getInstanceId())
                .metaId(publishedWebService.getMetaId())
                .uid(publishedWebService.getUid())));
        sourceDistribution.setStatus(StatusType.PUBLISHED);
        Distribution publishedDistribution = retrieveDistribution(EPOSDataModelManager.createEposDataModelEntity(
                sourceDistribution, admin, EntityNames.DISTRIBUTION, Distribution.class).getEntity());

        DataProduct payload = readUiPayload();
        payload.setDistribution(List.of(new LinkedEntity()
                .entityType(EntityNames.DISTRIBUTION.name())
                .instanceId(publishedDistribution.getInstanceId())
                .metaId(publishedDistribution.getMetaId())
                .uid(publishedDistribution.getUid())));
        payload.setStatus(StatusType.PUBLISHED);
        DataProduct published = retrieve(EPOSDataModelManager.createEposDataModelEntity(
                payload, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());

        DataProduct draftRequest = retrieve(published);
        draftRequest.setStatus(StatusType.DRAFT);
        DataProduct draft = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draftRequest, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());
        LinkedEntity draftDistributionLink = draft.getDistribution().get(0);
        Distribution draftDistribution = retrieveDistribution(draftDistributionLink);

        assertNotEquals(publishedDistribution.getInstanceId(), draftDistribution.getInstanceId());
        assertEquals(StatusType.DRAFT, draftDistribution.getStatus());
        WebService draftWebService = retrieveWebService(draftDistribution.getAccessService().get(0));
        assertNotEquals(publishedWebService.getInstanceId(), draftWebService.getInstanceId());
        assertEquals(StatusType.DRAFT, draftWebService.getStatus());
        Operation draftOperation = retrieveOperation(draftWebService.getSupportedOperation().get(0));
        assertNotEquals(publishedOperation.getInstanceId(), draftOperation.getInstanceId());
        assertEquals(StatusType.DRAFT, draftOperation.getStatus());
        Mapping draftMapping = retrieveMapping(draftOperation.getMapping().get(0));
        assertNotEquals(publishedMapping.getInstanceId(), draftMapping.getInstanceId());
        assertEquals(StatusType.DRAFT, draftMapping.getStatus());

        draftDistribution.setTitle(List.of("Modified draft distribution title"));
        draftDistribution.setDescription(List.of("Modified draft distribution description"));
        Distribution modifiedDraftDistribution = retrieveDistribution(EPOSDataModelManager.updateEposDataModelEntity(
                draftDistribution, admin, EntityNames.DISTRIBUTION, Distribution.class).getEntity());

        draft.setDistribution(List.of(new LinkedEntity()
                .entityType(EntityNames.DISTRIBUTION.name())
                .instanceId(modifiedDraftDistribution.getInstanceId())
                .metaId(modifiedDraftDistribution.getMetaId())
                .uid(modifiedDraftDistribution.getUid())));
        draft.setStatus(StatusType.SUBMITTED);
        DataProduct submitted = retrieve(EPOSDataModelManager.updateEposDataModelEntity(
                draft, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());
        Distribution submittedDistribution = retrieveDistribution(submitted.getDistribution().get(0));

        assertEquals(modifiedDraftDistribution.getInstanceId(), submittedDistribution.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submittedDistribution.getStatus());
        assertEquals("Modified draft distribution title", submittedDistribution.getTitle().get(0));
        assertEquals("Modified draft distribution description", submittedDistribution.getDescription().get(0));
        WebService submittedWebService = retrieveWebService(submittedDistribution.getAccessService().get(0));
        assertEquals(draftWebService.getInstanceId(), submittedWebService.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submittedWebService.getStatus());
        Operation submittedOperation = retrieveOperation(submittedWebService.getSupportedOperation().get(0));
        assertEquals(draftOperation.getInstanceId(), submittedOperation.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submittedOperation.getStatus());
        Mapping submittedMapping = retrieveMapping(submittedOperation.getMapping().get(0));
        assertEquals(draftMapping.getInstanceId(), submittedMapping.getInstanceId());
        assertEquals(StatusType.SUBMITTED, submittedMapping.getStatus());
    }

    @Test
    void transitionsSpatialAndTemporalExtentsForDataProductAndWebService() throws Exception {
        Location sourceLocation = new Location();
        sourceLocation.setUid("https://example.org/location/ui-lifecycle");
        sourceLocation.setLocation("POINT(12.5 41.9)");
        sourceLocation.setStatus(StatusType.PUBLISHED);
        Location publishedLocation = retrieveLocation(EPOSDataModelManager.createEposDataModelEntity(
                sourceLocation, admin, EntityNames.LOCATION, Location.class).getEntity());

        PeriodOfTime sourcePeriod = new PeriodOfTime();
        sourcePeriod.setUid("https://example.org/period/ui-lifecycle");
        sourcePeriod.setStartDate(LocalDateTime.of(2020, 1, 1, 0, 0));
        sourcePeriod.setEndDate(LocalDateTime.of(2020, 12, 31, 0, 0));
        sourcePeriod.setStatus(StatusType.PUBLISHED);
        PeriodOfTime publishedPeriod = retrievePeriod(EPOSDataModelManager.createEposDataModelEntity(
                sourcePeriod, admin, EntityNames.PERIODOFTIME, PeriodOfTime.class).getEntity());

        DataProduct sourceDataProduct = readUiPayload();
        sourceDataProduct.setStatus(StatusType.PUBLISHED);
        sourceDataProduct.setSpatialExtent(List.of(link(publishedLocation, EntityNames.LOCATION)));
        sourceDataProduct.setTemporalExtent(List.of(link(publishedPeriod, EntityNames.PERIODOFTIME)));
        DataProduct publishedDataProduct = retrieve(EPOSDataModelManager.createEposDataModelEntity(
                sourceDataProduct, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity());
        DataProduct draftDataProduct = transitionToDraft(publishedDataProduct, EntityNames.DATAPRODUCT, DataProduct.class);
        assertExtentStatuses(draftDataProduct.getSpatialExtent().get(0), draftDataProduct.getTemporalExtent().get(0), StatusType.DRAFT);
        DataProduct submittedDataProduct = transitionToSubmitted(draftDataProduct, EntityNames.DATAPRODUCT, DataProduct.class);
        assertExtentStatuses(submittedDataProduct.getSpatialExtent().get(0), submittedDataProduct.getTemporalExtent().get(0), StatusType.SUBMITTED);

        WebService sourceWebService = new WebService();
        sourceWebService.setUid("https://example.org/webservice/extents-ui-lifecycle");
        sourceWebService.setName("WebService extents lifecycle");
        sourceWebService.setStatus(StatusType.PUBLISHED);
        sourceWebService.setSpatialExtent(new ArrayList<>(List.of(link(publishedLocation, EntityNames.LOCATION))));
        sourceWebService.setTemporalExtent(List.of(link(publishedPeriod, EntityNames.PERIODOFTIME)));
        WebService publishedWebService = retrieveWebService(EPOSDataModelManager.createEposDataModelEntity(
                sourceWebService, admin, EntityNames.WEBSERVICE, WebService.class).getEntity());
        WebService draftWebService = transitionToDraft(publishedWebService, EntityNames.WEBSERVICE, WebService.class);
        assertExtentStatuses(draftWebService.getSpatialExtent().get(0), draftWebService.getTemporalExtent().get(0), StatusType.DRAFT);
        WebService submittedWebService = transitionToSubmitted(draftWebService, EntityNames.WEBSERVICE, WebService.class);
        assertExtentStatuses(submittedWebService.getSpatialExtent().get(0), submittedWebService.getTemporalExtent().get(0), StatusType.SUBMITTED);
    }

    private DataProduct readUiPayload() throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/fixtures/ui-dataproduct.json")) {
            assertNotNull(input);
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .registerModule(new ThreeTenModule());
            return mapper.readValue(input, DataProduct[].class)[0];
        }
    }

    @SuppressWarnings("unchecked")
    private DataProduct retrieve(LinkedEntity link) {
        return (DataProduct) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.DATAPRODUCT, DataProduct.class)
                .getListOfEntities().get(0);
    }

    private DataProduct retrieve(DataProduct dataProduct) {
        return retrieve(new LinkedEntity().metaId(dataProduct.getMetaId()).instanceId(dataProduct.getInstanceId()));
    }

    @SuppressWarnings("unchecked")
    private Distribution retrieveDistribution(LinkedEntity link) {
        return (Distribution) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.DISTRIBUTION, Distribution.class)
                .getListOfEntities().get(0);
    }

    @SuppressWarnings("unchecked")
    private WebService retrieveWebService(LinkedEntity link) {
        return (WebService) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.WEBSERVICE, WebService.class)
                .getListOfEntities().get(0);
    }

    @SuppressWarnings("unchecked")
    private Operation retrieveOperation(LinkedEntity link) {
        return (Operation) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.OPERATION, Operation.class)
                .getListOfEntities().get(0);
    }

    @SuppressWarnings("unchecked")
    private Mapping retrieveMapping(LinkedEntity link) {
        return (Mapping) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.MAPPING, Mapping.class)
                .getListOfEntities().get(0);
    }

    @SuppressWarnings("unchecked")
    private Location retrieveLocation(LinkedEntity link) {
        return (Location) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.LOCATION, Location.class)
                .getListOfEntities().get(0);
    }

    @SuppressWarnings("unchecked")
    private PeriodOfTime retrievePeriod(LinkedEntity link) {
        return (PeriodOfTime) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, EntityNames.PERIODOFTIME, PeriodOfTime.class)
                .getListOfEntities().get(0);
    }

    private LinkedEntity link(org.epos.eposdatamodel.EPOSDataModelEntity entity, EntityNames entityName) {
        return new LinkedEntity().entityType(entityName.name()).instanceId(entity.getInstanceId())
                .metaId(entity.getMetaId()).uid(entity.getUid());
    }

    private <T extends org.epos.eposdatamodel.EPOSDataModelEntity> T transitionToDraft(
            T entity, EntityNames entityName, Class<T> entityClass) {
        entity.setStatus(StatusType.DRAFT);
        return retrieveEntity(EPOSDataModelManager.updateEposDataModelEntity(
                entity, admin, entityName, entityClass).getEntity(), entityName, entityClass);
    }

    private <T extends org.epos.eposdatamodel.EPOSDataModelEntity> T transitionToSubmitted(
            T entity, EntityNames entityName, Class<T> entityClass) {
        entity.setStatus(StatusType.SUBMITTED);
        return retrieveEntity(EPOSDataModelManager.updateEposDataModelEntity(
                entity, admin, entityName, entityClass).getEntity(), entityName, entityClass);
    }

    @SuppressWarnings("unchecked")
    private <T extends org.epos.eposdatamodel.EPOSDataModelEntity> T retrieveEntity(
            LinkedEntity link, EntityNames entityName, Class<T> entityClass) {
        return (T) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, entityName, entityClass)
                .getListOfEntities().get(0);
    }

    private void assertExtentStatuses(LinkedEntity spatialLink, LinkedEntity temporalLink, StatusType expectedStatus) {
        assertEquals(expectedStatus, retrieveLocation(spatialLink).getStatus());
        assertEquals(expectedStatus, retrievePeriod(temporalLink).getStatus());
    }

    private void assertVersions(String uid, String publishedInstanceId, String draftInstanceId, StatusType draftStatus) {
        List<Versioningstatus> versions = EposDataModelDAO.getInstance()
                .getOneFromDBByUIDNoCache(uid, Versioningstatus.class);
        assertEquals(2, versions.size());
        assertEquals(1, versions.stream()
                .filter(version -> draftInstanceId.equals(version.getInstanceId()))
                .filter(version -> draftStatus.name().equals(version.getStatus()))
                .count());
        assertEquals(1, versions.stream()
                .filter(version -> publishedInstanceId.equals(version.getInstanceId()))
                .count());
    }
}
