package org.epos.backoffice.api.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.Mapping;
import org.epos.eposdatamodel.Operation;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import commonapis.LinkedEntityAPI;
import metadataapis.EntityNames;
import model.StatusType;

public class EntityManagementOperationMappingTest extends TestcontainersLifecycle {

    static User user = null;

    @Test
    @Order(1)
    public void testCreateUser() {
        user = new User("testid", "familyname", "givenname", "email@email.email", true);
        UserManager.createUser(user, user);

        User retrieveUser = UserManager.getUser(user.getAuthIdentifier(),user,false).getListOfUsers().get(0);

        assertNotNull(retrieveUser);
        assertEquals(user.getAuthIdentifier(), retrieveUser.getAuthIdentifier());
        assertEquals(user.getLastName(), retrieveUser.getLastName());
        assertEquals(user.getFirstName(), retrieveUser.getFirstName());
        assertEquals(user.getEmail(), retrieveUser.getEmail());
    }

    @Test
    @Order(2)
    public void testCreateAndGet() {

        Operation operation = new Operation();
        operation.setMethod("GET");
        operation.setTemplate("http://template{?test,test1}");
        operation.setReturns(List.of("application/json"));
        operation.setStatus(StatusType.DRAFT);

        Mapping mapping1 = new Mapping();
        mapping1.setVariable("test1");
        mapping1.setLabel("label1");
        mapping1.setStatus(StatusType.DRAFT);

        LinkedEntity mapping1LinkedEntity = EPOSDataModelManager.createEposDataModelEntity(mapping1, user, EntityNames.MAPPING, Mapping.class).getEntity();

        operation.setMapping(List.of(mapping1LinkedEntity));

        System.out.println(operation);
        LinkedEntity operationLinkedEntity = EPOSDataModelManager.createEposDataModelEntity(operation, user, EntityNames.OPERATION, Operation.class).getEntity();

        assertEquals(1, ((Operation)LinkedEntityAPI.retrieveFromLinkedEntity(operationLinkedEntity)).getMapping().size());

    }

}
