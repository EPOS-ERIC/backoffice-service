package org.epos.backoffice.api.controller;

import metadataapis.EntityNames;
import model.StatusType;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.Distribution;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.Operation;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.WebService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SharedWebServiceDataProductDraftTest extends TestcontainersLifecycle {

    private static User admin;

    @BeforeAll
    static void createAdmin() {
        admin = new User("shared-graph-admin-" + UUID.randomUUID(), "Admin", "SharedGraph",
                "shared-graph@example.org", true);
        UserManager.createUser(admin, admin);
    }

    @Test
    void draftsTheCompleteSharedGraphWithoutOverwritingPublishedValues() {
        LinkedEntity operation1 = createOperation("operation1", "https://example.org/operation1");
        LinkedEntity operation2 = createOperation("operation2", "https://example.org/operation2");
        LinkedEntity webService = createWebService(operation1, operation2);
        LinkedEntity distribution1 = createDistribution("distribution1", webService, operation1);
        LinkedEntity distribution2 = createDistribution("distribution2", webService, operation2);
        LinkedEntity dataset1 = createDataProduct("dataset1", distribution1);
        createDataProduct("dataset2", distribution2);

        DataProduct publishedDataset1 = retrieve(dataset1, EntityNames.DATAPRODUCT, DataProduct.class);
        publishedDataset1.setStatus(StatusType.DRAFT);
        LinkedEntity dataset1DraftLink = EPOSDataModelManager.updateEposDataModelEntity(
                publishedDataset1, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity();
        DataProduct draftDataset1 = retrieve(dataset1DraftLink, EntityNames.DATAPRODUCT, DataProduct.class);



        assertEquals(StatusType.DRAFT, draftDataset1.getStatus());
        assertEquals("dataset1", draftDataset1.getTitle().get(0));
        assertNotEquals(dataset1.getInstanceId(), draftDataset1.getInstanceId());

        Distribution draftDistribution1 = retrieve(draftDataset1.getDistribution().get(0),
                EntityNames.DISTRIBUTION, Distribution.class);
        assertEquals(StatusType.DRAFT, draftDistribution1.getStatus());
        assertEquals("distribution1", draftDistribution1.getTitle().get(0));
        assertNotEquals(distribution1.getInstanceId(), draftDistribution1.getInstanceId());

        WebService draftWebService = retrieve(draftDistribution1.getAccessService().get(0),
                EntityNames.WEBSERVICE, WebService.class);
        assertEquals(StatusType.DRAFT, draftWebService.getStatus());
        assertEquals("shared-webservice", draftWebService.getName());
        assertEquals(2, draftWebService.getSupportedOperation().size());
        assertNotEquals(webService.getInstanceId(), draftWebService.getInstanceId());

        for (LinkedEntity operationLink : draftWebService.getSupportedOperation()) {
            Operation draftOperation = retrieve(operationLink, EntityNames.OPERATION, Operation.class);
            assertEquals(StatusType.DRAFT, draftOperation.getStatus());
            assertEquals("GET", draftOperation.getMethod());
        }

        draftDataset1.setTitle(List.of("dataset1-draft"));
        LinkedEntity updatedDatasetLink = EPOSDataModelManager.updateEposDataModelEntity(
                draftDataset1, admin, EntityNames.DATAPRODUCT, DataProduct.class).getEntity();
        DataProduct afterDatasetUpdate = retrieve(updatedDatasetLink, EntityNames.DATAPRODUCT, DataProduct.class);
        assertNotNull(afterDatasetUpdate.getDistribution(), "DataProduct lost distribution immediately after title update");

        draftDistribution1.setTitle(List.of("distribution1-draft"));
        LinkedEntity updatedDistributionLink = EPOSDataModelManager.updateEposDataModelEntity(
                draftDistribution1, admin, EntityNames.DISTRIBUTION, Distribution.class).getEntity();
        Distribution afterDistributionUpdate = retrieve(updatedDistributionLink, EntityNames.DISTRIBUTION, Distribution.class);
        assertNotNull(afterDistributionUpdate.getAccessService(),
                "Distribution lost accessService immediately after title update");

        draftWebService.setName("shared-webservice-draft");
        EPOSDataModelManager.updateEposDataModelEntity(
                draftWebService, admin, EntityNames.WEBSERVICE, WebService.class);
        

        assertEquals("dataset1", retrieve(dataset1, EntityNames.DATAPRODUCT, DataProduct.class).getTitle().get(0));
        assertEquals("distribution1", retrieve(distribution1, EntityNames.DISTRIBUTION, Distribution.class).getTitle().get(0));
        assertEquals("shared-webservice", retrieve(webService, EntityNames.WEBSERVICE, WebService.class).getName());
        DataProduct reloadedDraftDataset = retrieve(dataset1DraftLink, EntityNames.DATAPRODUCT, DataProduct.class);
        assertEquals("dataset1-draft", reloadedDraftDataset.getTitle().get(0));
        assertNotNull(reloadedDraftDataset.getDistribution(), "DataProduct draft lost distribution after title update");
        Distribution reloadedDraftDistribution = retrieve(reloadedDraftDataset.getDistribution().get(0),
                EntityNames.DISTRIBUTION, Distribution.class);
        assertEquals("distribution1-draft", reloadedDraftDistribution.getTitle().get(0));
        assertNotNull(reloadedDraftDistribution.getAccessService(),
                "Distribution draft lost accessService after title update");
        assertEquals("shared-webservice-draft", retrieve(reloadedDraftDistribution.getAccessService().get(0),
                EntityNames.WEBSERVICE, WebService.class).getName());

        // Publish the edited branch, replacing draft links with the published versions first.
        List<LinkedEntity> publishedOperations = draftWebService.getSupportedOperation().stream()
                .map(link -> publishLink(link, EntityNames.OPERATION, Operation.class))
                .toList();
        draftWebService.setSupportedOperation(publishedOperations);
        LinkedEntity publishedWebService = publishLink(updateDraft(draftWebService, EntityNames.WEBSERVICE, WebService.class),
                EntityNames.WEBSERVICE, WebService.class);

        draftDistribution1.setAccessService(List.of(publishedWebService));
        draftDistribution1.setSupportedOperation(publishedOperations);
        LinkedEntity publishedDistribution1 = publishLink(updateDraft(draftDistribution1, EntityNames.DISTRIBUTION, Distribution.class),
                EntityNames.DISTRIBUTION, Distribution.class);


        draftDataset1.setDistribution(List.of(publishedDistribution1));
        LinkedEntity publishedDatasetLink = publishLink(updateDraft(draftDataset1, EntityNames.DATAPRODUCT, DataProduct.class),
                EntityNames.DATAPRODUCT, DataProduct.class);
        DataProduct publishedDataset = retrieve(publishedDatasetLink, EntityNames.DATAPRODUCT, DataProduct.class);

        assertEquals(StatusType.PUBLISHED, publishedDataset.getStatus());
        assertEquals(StatusType.PUBLISHED, retrieve(publishedDistribution1, EntityNames.DISTRIBUTION, Distribution.class).getStatus());
        assertEquals(StatusType.PUBLISHED, retrieve(publishedWebService, EntityNames.WEBSERVICE, WebService.class).getStatus());

    }

    private LinkedEntity updateDraft(org.epos.eposdatamodel.EPOSDataModelEntity entity,
                                     EntityNames entityName, Class<?> entityClass) {
        return EPOSDataModelManager.updateEposDataModelEntity(entity, admin, entityName, entityClass).getEntity();
    }

    private <T extends org.epos.eposdatamodel.EPOSDataModelEntity> LinkedEntity publishLink(
            LinkedEntity draftLink, EntityNames entityName, Class<T> entityClass) {
        T draft = retrieve(draftLink, entityName, entityClass);
        draft.setStatus(StatusType.SUBMITTED);
        LinkedEntity submittedLink = updateDraft(draft, entityName, entityClass);
        T submitted = retrieve(submittedLink, entityName, entityClass);
        submitted.setStatus(StatusType.PUBLISHED);
        return updateDraft(submitted, entityName, entityClass);
    }

    private LinkedEntity createOperation(String name, String template) {
        Operation operation = new Operation();
        operation.setUid("test:shared:" + name + ":" + UUID.randomUUID());
        operation.setMethod("GET");
        operation.setTemplate(template);
        operation.setStatus(StatusType.PUBLISHED);
        return create(operation, EntityNames.OPERATION, Operation.class);
    }

    private LinkedEntity createWebService(LinkedEntity operation1, LinkedEntity operation2) {
        WebService webService = new WebService();
        webService.setUid("test:shared:webservice:" + UUID.randomUUID());
        webService.setName("shared-webservice");
        webService.setSupportedOperation(List.of(operation1, operation2));
        webService.setStatus(StatusType.PUBLISHED);
        return create(webService, EntityNames.WEBSERVICE, WebService.class);
    }

    private LinkedEntity createDistribution(String name, LinkedEntity webService, LinkedEntity operation) {
        Distribution distribution = new Distribution();
        distribution.setUid("test:shared:" + name + ":" + UUID.randomUUID());
        distribution.setTitle(List.of(name));
        distribution.setFormat("application/json");
        distribution.setAccessService(List.of(webService));
        distribution.setSupportedOperation(List.of(operation));
        distribution.setStatus(StatusType.PUBLISHED);
        return create(distribution, EntityNames.DISTRIBUTION, Distribution.class);
    }

    private LinkedEntity createDataProduct(String name, LinkedEntity distribution) {
        DataProduct dataProduct = new DataProduct();
        dataProduct.setUid("test:shared:" + name + ":" + UUID.randomUUID());
        dataProduct.setTitle(List.of(name));
        dataProduct.setDistribution(List.of(distribution));
        dataProduct.setStatus(StatusType.PUBLISHED);
        return create(dataProduct, EntityNames.DATAPRODUCT, DataProduct.class);
    }

    private <T extends org.epos.eposdatamodel.EPOSDataModelEntity> LinkedEntity create(
            T entity, EntityNames entityName, Class<T> entityClass) {
        return EPOSDataModelManager.createEposDataModelEntity(entity, admin, entityName, entityClass).getEntity();
    }

    @SuppressWarnings("unchecked")
    private <T> T retrieve(LinkedEntity link, EntityNames entityName, Class<T> entityClass) {
        assertNotNull(link);
        return (T) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(
                link.getMetaId(), link.getInstanceId(), admin, entityName, entityClass)
                .getListOfEntities().get(0);
    }
}
