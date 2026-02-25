package org.epos.backoffice.api.controller;

import metadataapis.EntityNames;
import model.RequestStatusType;
import model.RoleType;
import model.StatusType;
import org.epos.backoffice.api.util.*;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.Identifier;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import usermanagementapis.UserGroupManagementAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class UserGroupManagementTest extends TestcontainersLifecycle {

    static User user;
    static Group group;

    @Test
    @Order(1)
    public void testCreateUser() {
        user = new User("testid", "familyname", "givenname", "email@email.email", true);
        UserManager.createUser(user,user);

        User retrieveUser = UserManager.getUser(user.getAuthIdentifier(),user,false).getListOfUsers().get(0);

        assertNotNull(retrieveUser);
        assertEquals(user.getAuthIdentifier(), retrieveUser.getAuthIdentifier());
        assertEquals(user.getLastName(), retrieveUser.getLastName());
        assertEquals(user.getFirstName(), retrieveUser.getFirstName());
        assertEquals(user.getEmail(), retrieveUser.getEmail());
    }

    @Test
    @Order(2)
    public void testUpdateUser() {

        user.setEmail("newemail@email.email");
        user.setLastName("newfamilyname");
        user.setFirstName("newgivenname");

        UserManager.createUser(user,user);

        User retrieveUser = UserManager.getUser(user.getAuthIdentifier(),user,false).getListOfUsers().get(0);

        assertNotNull(retrieveUser);
        assertEquals(user.getAuthIdentifier(), retrieveUser.getAuthIdentifier());
        assertEquals(user.getLastName(), retrieveUser.getLastName());
        assertEquals(user.getFirstName(), retrieveUser.getFirstName());
        assertEquals(user.getEmail(), retrieveUser.getEmail());
    }

    @Test
    @Order(3)
    public void testCreateGroup() {
        group = new Group(UUID.randomUUID().toString(), "Test Group", "Test Decription");
        GroupManager.createGroup(group, user);

        Group retrieveGroup = GroupManager.getGroup(group.getId(), user,false).getListOfGroups().get(0);

        assertNotNull(retrieveGroup);
        assertEquals(group.getId(), retrieveGroup.getId());
        assertEquals(group.getName(), retrieveGroup.getName());
        assertEquals(group.getDescription(), retrieveGroup.getDescription());
    }

    @Test
    @Order(4)
    public void testUpdateGroup() {

        group.setDescription("Test updated description");

        GroupManager.createGroup(group, user);

        Group retrieveGroup = GroupManager.getGroup(group.getId(), user,false).getListOfGroups().get(0);

        assertNotNull(retrieveGroup);
        assertEquals(group.getId(), retrieveGroup.getId());
        assertEquals(group.getName(), retrieveGroup.getName());
        assertEquals(group.getDescription(), retrieveGroup.getDescription());
    }

    @Test
    @Order(5)
    public void testAddUserToGroup() {
        AddUserToGroupBean addUserToGroupBean = new AddUserToGroupBean();
        addUserToGroupBean.setGroupid(group.getId());
        addUserToGroupBean.setUserid(user.getAuthIdentifier());
        addUserToGroupBean.setRole(RoleType.EDITOR.toString());
        addUserToGroupBean.setStatusType(RequestStatusType.PENDING.toString());

        UserManager.addUserToGroup(addUserToGroupBean,user);

        Group retrieveGroup = GroupManager.getGroup(group.getId(), user, false).getListOfGroups().get(0);
        User retrieveUser = UserManager.getUser(user.getAuthIdentifier(),user,true).getListOfUsers().get(0);

        assertAll(
                () -> assertNotNull(retrieveGroup),
                () -> assertEquals(1, retrieveGroup.getUsers().size()),
                () -> assertEquals(retrieveGroup.getUsers().get(0).get("userId"), retrieveUser.getAuthIdentifier()),
                () -> assertEquals(1, retrieveUser.getGroups().size()),
                () -> assertEquals(retrieveUser.getGroups().get(0).getGroupId(), retrieveGroup.getId()),
                () -> assertEquals(retrieveUser.getGroups().get(0).getRole(), RoleType.EDITOR)
        );
    }

    @Test
    @Order(6)
    public void testAddSameUserToGroup() {
        AddUserToGroupBean addUserToGroupBean = new AddUserToGroupBean();
        addUserToGroupBean.setGroupid(group.getId());
        addUserToGroupBean.setUserid(user.getAuthIdentifier());
        addUserToGroupBean.setRole(RoleType.EDITOR.toString());
        addUserToGroupBean.setStatusType(RequestStatusType.PENDING.toString());

        UserManager.addUserToGroup(addUserToGroupBean,user);

        Group retrieveGroup = GroupManager.getGroup(group.getId(), user, false).getListOfGroups().get(0);
        User retrieveUser = UserManager.getUser(user.getAuthIdentifier(),user,false).getListOfUsers().get(0);

        System.out.println(retrieveGroup);
        System.out.println(retrieveUser);

        assertAll(
                () -> assertNotNull(retrieveGroup),
                () -> assertEquals(1, retrieveGroup.getUsers().size()),
                () -> assertEquals(retrieveGroup.getUsers().get(0).get("userId"), retrieveUser.getAuthIdentifier()),
                () -> assertEquals(1, retrieveUser.getGroups().size()),
                () -> assertEquals(retrieveUser.getGroups().get(0).getGroupId(), retrieveGroup.getId()),
                () -> assertEquals(retrieveUser.getGroups().get(0).getRole(), RoleType.EDITOR)
        );
    }

    @Test
    @Order(7)
    public void testRemoveUserFromGroup() {
        // First, ensure user is in the group (from previous test)
        User retrieveUserBefore = UserManager.getUser(user.getAuthIdentifier(), user, true).getListOfUsers().get(0);
        Group retrieveGroupBefore = GroupManager.getGroup(group.getId(), user, false).getListOfGroups().get(0);
        
        System.out.println("Before removal - User groups: " + retrieveUserBefore.getGroups());
        System.out.println("Before removal - Group users: " + retrieveGroupBefore.getUsers());
        
        // Verify user is in the group before removal
        assertEquals(1, retrieveUserBefore.getGroups().size(), "User should be in 1 group before removal");
        assertEquals(1, retrieveGroupBefore.getUsers().size(), "Group should have 1 user before removal");
        
        // Remove user from group
        RemoveUserFromGroupBean removeUserFromGroupBean = new RemoveUserFromGroupBean();
        removeUserFromGroupBean.setGroupid(group.getId());
        removeUserFromGroupBean.setUserid(user.getAuthIdentifier());
        
        var response = UserManager.removeUserFromGroup(removeUserFromGroupBean, user);
        System.out.println("Remove response: " + response);
        
        // Verify the user is no longer in the group
        User retrieveUserAfter = UserManager.getUser(user.getAuthIdentifier(), user, true).getListOfUsers().get(0);
        Group retrieveGroupAfter = GroupManager.getGroup(group.getId(), user, false).getListOfGroups().get(0);
        
        System.out.println("After removal - User groups: " + retrieveUserAfter.getGroups());
        System.out.println("After removal - Group users: " + retrieveGroupAfter.getUsers());
        
        assertAll(
                () -> assertNotNull(retrieveUserAfter),
                () -> assertEquals(0, retrieveUserAfter.getGroups().size(), "User should have 0 groups after removal"),
                () -> assertNotNull(retrieveGroupAfter),
                () -> assertEquals(0, retrieveGroupAfter.getUsers().size(), "Group should have 0 users after removal")
        );
    }

    @Test
    @Order(8)
    public void testRemoveUserFromGroupWithEntity() {
        // Create a new group for this test
        Group groupWithEntity = new Group(UUID.randomUUID().toString(), "Group With Entity", "Test group with entity");
        GroupManager.createGroup(groupWithEntity, user);
        
        // Add user to the group as EDITOR
        AddUserToGroupBean addUserToGroupBean = new AddUserToGroupBean();
        addUserToGroupBean.setGroupid(groupWithEntity.getId());
        addUserToGroupBean.setUserid(user.getAuthIdentifier());
        addUserToGroupBean.setRole(RoleType.EDITOR.toString());
        addUserToGroupBean.setStatusType(RequestStatusType.ACCEPTED.toString());
        UserManager.addUserToGroup(addUserToGroupBean, user);
        
        // Create an entity (Identifier) and add it to the group
        Identifier identifier = new Identifier();
        identifier.setInstanceId(UUID.randomUUID().toString());
        identifier.setMetaId(UUID.randomUUID().toString());
        identifier.setUid(UUID.randomUUID().toString());
        identifier.setType("TYPE");
        identifier.setIdentifier("012345678900");
        
        LinkedEntity identifierLe = EPOSDataModelManager.createEposDataModelEntity(
            identifier, user, EntityNames.IDENTIFIER, Identifier.class).getEntity();
        
        // Add the entity to the group
        AddEntityToGroupBean addEntityToGroupBean = new AddEntityToGroupBean();
        addEntityToGroupBean.setGroupid(groupWithEntity.getId());
        addEntityToGroupBean.setMetaid(identifierLe.getMetaId());
        GroupManager.addEntityToGroup(addEntityToGroupBean, user);
        
        // Verify setup: user is in group and group has entity
        Group retrieveGroupBefore = GroupManager.getGroup(groupWithEntity.getId(), user, false).getListOfGroups().get(0);
        System.out.println("Before removal - Group users: " + retrieveGroupBefore.getUsers());
        System.out.println("Before removal - Group entities: " + retrieveGroupBefore.getEntities());
        
        assertEquals(1, retrieveGroupBefore.getUsers().size(), "Group should have 1 user before removal");
        assertEquals(1, retrieveGroupBefore.getEntities().size(), "Group should have 1 entity before removal");
        
        // Now try to remove user from the group (this is where the issue should occur)
        RemoveUserFromGroupBean removeUserFromGroupBean = new RemoveUserFromGroupBean();
        removeUserFromGroupBean.setGroupid(groupWithEntity.getId());
        removeUserFromGroupBean.setUserid(user.getAuthIdentifier());
        
        var response = UserManager.removeUserFromGroup(removeUserFromGroupBean, user);
        System.out.println("Remove user from group with entity - Response: " + response);
        
        // Verify the result
        assertEquals(ApiResponseMessage.OK, response.getCode(), 
            "Should be able to remove user from group even when group has entities. Got: " + response.getMessage());
        
        // Verify the user is no longer in the group
        Group retrieveGroupAfter = GroupManager.getGroup(groupWithEntity.getId(), user, false).getListOfGroups().get(0);
        System.out.println("After removal - Group users: " + retrieveGroupAfter.getUsers());
        System.out.println("After removal - Group entities: " + retrieveGroupAfter.getEntities());
        
        assertAll(
            () -> assertEquals(0, retrieveGroupAfter.getUsers().size(), "Group should have 0 users after removal"),
            () -> assertEquals(1, retrieveGroupAfter.getEntities().size(), "Group should still have 1 entity after user removal")
        );
        
        // Cleanup: delete the group
        GroupManager.deleteGroup(groupWithEntity.getId(), user);
    }

    @Test
    @Order(9)
    public void testRemoveEditorUserFromGroupWithDraftDataProduct() {
        // This test reproduces the exact scenario:
        // 1. User is EDITOR with ACCEPTED status in a group
        // 2. User creates a DRAFT DataProduct in that group
        // 3. Admin tries to remove the user from the group
        // Expected: Should succeed (or return meaningful error)
        
        // Ensure we have an admin user (if previous tests didn't run)
        if (user == null) {
            user = new User("testid", "familyname", "givenname", "email@email.email", true);
            UserManager.createUser(user, user);
        }
        
        // Create a new group for this test
        Group groupWithDraft = new Group(UUID.randomUUID().toString(), "Group With Draft DataProduct", "Test group");
        GroupManager.createGroup(groupWithDraft, user);
        
        // Create a non-admin user to be the editor
        User editorUser = new User("editor-user-id", "Editor", "User", "editor@email.email", false);
        UserManager.createUser(editorUser, user);
        
        // Add the editor user to the group as EDITOR with ACCEPTED status
        AddUserToGroupBean addUserToGroupBean = new AddUserToGroupBean();
        addUserToGroupBean.setGroupid(groupWithDraft.getId());
        addUserToGroupBean.setUserid(editorUser.getAuthIdentifier());
        addUserToGroupBean.setRole(RoleType.EDITOR.toString());
        addUserToGroupBean.setStatusType(RequestStatusType.ACCEPTED.toString());
        UserManager.addUserToGroup(addUserToGroupBean, user);
        
        // Create a DRAFT DataProduct with the editor user as the creator/editor
        DataProduct dataProduct = new DataProduct();
        dataProduct.setInstanceId(UUID.randomUUID().toString());
        dataProduct.setMetaId(UUID.randomUUID().toString());
        dataProduct.setUid(UUID.randomUUID().toString());
        dataProduct.setTitle(List.of("Test Draft DataProduct"));
        dataProduct.setDescription(List.of("Test Description"));
        dataProduct.setStatus(StatusType.DRAFT);
        dataProduct.setGroups(List.of(groupWithDraft.getId()));
        
        // Create the DataProduct as the editor user (not admin)
        ApiResponseMessage createResponse = EPOSDataModelManager.createEposDataModelEntity(
            dataProduct, editorUser, EntityNames.DATAPRODUCT, DataProduct.class);
        System.out.println("Create DataProduct response: " + createResponse);
        
        LinkedEntity dataProductLe = createResponse.getEntity();
        assertNotNull(dataProductLe, "DataProduct should be created successfully");
        
        // Verify setup
        Group retrieveGroupBefore = GroupManager.getGroup(groupWithDraft.getId(), user, false).getListOfGroups().get(0);
        System.out.println("Before removal - Group users: " + retrieveGroupBefore.getUsers());
        System.out.println("Before removal - Group entities: " + retrieveGroupBefore.getEntities());
        
        assertEquals(1, retrieveGroupBefore.getUsers().size(), "Group should have 1 user (editor) before removal");
        assertTrue(retrieveGroupBefore.getEntities().size() >= 1, "Group should have at least 1 entity (DataProduct) before removal");
        
        // Now try to remove the editor user from the group (admin doing the removal)
        RemoveUserFromGroupBean removeUserFromGroupBean = new RemoveUserFromGroupBean();
        removeUserFromGroupBean.setGroupid(groupWithDraft.getId());
        removeUserFromGroupBean.setUserid(editorUser.getAuthIdentifier());
        
        var response = UserManager.removeUserFromGroup(removeUserFromGroupBean, user);
        System.out.println("Remove editor from group with DRAFT DataProduct - Response: " + response);
        
        // Verify the result - this is the key assertion!
        assertEquals(ApiResponseMessage.OK, response.getCode(), 
            "Should be able to remove editor user from group even when they have DRAFT entities. Got: " + response.getMessage());
        
        // Verify the user is no longer in the group
        Group retrieveGroupAfter = GroupManager.getGroup(groupWithDraft.getId(), user, false).getListOfGroups().get(0);
        System.out.println("After removal - Group users: " + retrieveGroupAfter.getUsers());
        System.out.println("After removal - Group entities: " + retrieveGroupAfter.getEntities());
        
        assertEquals(0, retrieveGroupAfter.getUsers().size(), "Group should have 0 users after removal");
        
        // Cleanup
        UserManager.deleteUser(editorUser.getAuthIdentifier(), user);
        GroupManager.deleteGroup(groupWithDraft.getId(), user);
    }

    @Test
    @Order(10)
    public void testDeleteUser() {
        UserManager.deleteUser(user.getAuthIdentifier(), user);

        assertEquals(new ArrayList<User>(), UserManager.getUser(user.getAuthIdentifier(),user,false).getListOfUsers());

    }

    @Test
    @Order(11)
    public void testDeleteGroup() {

        GroupManager.deleteGroup(group.getId(), user);

        assertEquals(new ArrayList<User>(), GroupManager.getGroup(user.getAuthIdentifier(),user,false).getListOfGroups());
    }


    @Test
    @Order(12)
    public void testCreateGroupWithoutName() {
        Group group = new Group(UUID.randomUUID().toString(), null, "Test Decription");
        GroupManager.createGroup(group, user);

        Group retrieveGroup = GroupManager.getGroup(group.getId(),user,false).getListOfGroups().get(0);

        assertNotNull(retrieveGroup);
        assertEquals(group.getId(), retrieveGroup.getId());
        assertEquals(group.getName(), retrieveGroup.getName());
        assertEquals(group.getDescription(), retrieveGroup.getDescription());
    }
}
