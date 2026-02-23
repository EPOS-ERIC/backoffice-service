package org.epos.backoffice.api.util;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import model.MetadataGroupUser;
import model.RequestStatusType;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.Distribution;
import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.UserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.annotation.JsonProperty;

import abstractapis.AbstractAPI;
import commonapis.LinkedEntityAPI;
import dao.EposDataModelDAO;
import metadataapis.EntityNames;
import model.RoleType;
import model.StatusType;
import usermanagementapis.UserGroupManagementAPI;

import static abstractapis.AbstractRelationsAPI.getDbaccess;

public class EPOSDataModelManager {

    private static final Logger log = LoggerFactory.getLogger(EPOSDataModelManager.class);
    private static final RestTemplate restTemplate = new RestTemplate();

    public static ApiResponseMessage getEPOSDataModelEposDataModelEntity(String meta_id, String instance_id, User user, EntityNames entityNames, Class clazz) {

        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());
        if (meta_id == null)
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "{\"response\" : \"The [meta_id] field can't be left blank\"}");
        if(instance_id == null) {
            instance_id = "all";
        }

        /*
        TODO: check if this is needed
        if((entityNames.equals(EntityNames.PERSON)
                || entityNames.equals(EntityNames.ORGANIZATION)
                || entityNames.equals(EntityNames.CONTACTPOINT)) && !user.getIsAdmin()){
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "A user which is not Admin can't access PERSON/ORGANIZATION/CONTACTPOINT entities due to privacy settings");
        }*/

        List<EPOSDataModelEntity> list;
        
        // Pre-fetch user's group roles once for all permission checks (optimization)
        // For backoffice admins, we pass null since they have full access
        final Map<String, String> userGroupRoles = user.getIsAdmin() ? null : getUserAcceptedGroupRoles(user);
        
        if (meta_id.equals("all")) {
            // Retrieve all entities, only filter by user permissions
            list = dbapi.retrieveAll();
            list = list.stream()
                    .filter(elem -> checkUserPermissionsReadOnly(elem, user, userGroupRoles))
                    .collect(Collectors.toList());
        } else {
            if(instance_id.equals("all")) {
                // Retrieve all instances for a specific meta_id
                list = dbapi.retrieveAll();
                list = list.stream()
                        .filter(elem -> elem.getMetaId().equals(meta_id))
                        .filter(elem -> checkUserPermissionsReadOnly(elem, user, userGroupRoles))
                        .collect(Collectors.toList());
            } else {
                // Retrieve a specific instance
                list = new ArrayList<>();
                EPOSDataModelEntity entity = (EPOSDataModelEntity) dbapi.retrieve(instance_id);
                if(entity != null && entity.getMetaId().equals(meta_id) && checkUserPermissionsReadOnly(entity, user, userGroupRoles)) {
                    list.add(entity);
                }
            }
        }
        
        if (list.isEmpty())
            return new ApiResponseMessage(ApiResponseMessage.OK, new ArrayList<EPOSDataModelEntity>());

        return new ApiResponseMessage(ApiResponseMessage.OK, list);
    }

    public static ApiResponseMessage createEposDataModelEntity(EPOSDataModelEntity obj, User user, EntityNames entityNames, Class clazz) {

        EposDataModelDAO.getInstance().clearAllCaches();

        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());
        
        // Pre-fetch user's group roles once for all permission checks (optimization)
        final Map<String, String> userGroupRoles = user.getIsAdmin() ? null : getUserAcceptedGroupRoles(user);

        // If creating from an existing entity (e.g., draft from published), retrieve it first
        EPOSDataModelEntity existingEntity = null;
        if(obj.getInstanceId() != null) {
            existingEntity = (EPOSDataModelEntity) dbapi.retrieve(obj.getInstanceId());
            if(existingEntity != null) {
                // Check if user can READ the existing entity (to copy from it)
                if(!checkUserPermissionsReadOnly(existingEntity, user, userGroupRoles)) {
                    return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"The user can't read the source entity\"}");
                }
                // Inherit groups from existing entity if not specified
                if(obj.getGroups() == null || obj.getGroups().isEmpty()) {
                    obj.setGroups(existingEntity.getGroups());
                }
                // Set the target status for the new entity (default DRAFT)
                obj.setStatus(obj.getStatus() == null ? StatusType.DRAFT : obj.getStatus());
                // Check if user can CREATE the new entity with the target status
                if(!checkUserPermissionsReadWrite(obj, user, userGroupRoles)) {
                    return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"The user can't create an entity with this status\"}");
                }
            }
            obj.setInstanceChangedId(obj.getInstanceId());
        }
        
        // For brand new entities (no existing entity), determine groups
        if(existingEntity == null && (obj.getGroups() == null || obj.getGroups().isEmpty())) {
            // User didn't specify groups - use the user's groups where they have write permission
            List<String> userWritableGroups = getUserWritableGroups(user, userGroupRoles);
            
            if(userWritableGroups.isEmpty()) {
                // Fallback to ALL group for admins
                Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
                if(allGroup != null) {
                    obj.setGroups(List.of(allGroup.getId()));
                }
            } else {
                obj.setGroups(userWritableGroups);
            }
        }

        // For brand new entities, check permissions on the target groups
        if(existingEntity == null) {
            obj.setStatus(obj.getStatus() == null ? StatusType.DRAFT : obj.getStatus());
            if(!checkUserPermissionsReadWrite(obj, user, userGroupRoles)) {
                return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"The user can't create an entity with this status\"}");
            }
        }

        // Status already set above, but ensure it's set for all cases
        if(obj.getStatus() == null) {
            obj.setStatus(StatusType.DRAFT);
        }
        obj.setEditorId(user.getAuthIdentifier());
        obj.setFileProvenance("backoffice");

        LinkedEntity reference = dbapi.create(obj, null, null, null);

		if (obj instanceof DataProduct && obj.getInstanceChangedId() != null) {
			CompletableFuture.runAsync(() -> {
				updatePluginsForDraftDistribution(reference, (DataProduct) obj);
			});
		}

        if(obj.getGroups()!=null && !obj.getGroups().isEmpty()){
            for(String groupid : obj.getGroups()){
                UserGroupManagementAPI.addMetadataElementToGroup(reference.getMetaId(), groupid);
            }
        }

        return new ApiResponseMessage(ApiResponseMessage.OK, reference);
    }

    public static ApiResponseMessage updateEposDataModelEntity(EPOSDataModelEntity obj, User user, EntityNames entityNames, Class clazz) {
        EposDataModelDAO.getInstance().clearAllCaches();
        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());

        if (obj.getInstanceId() == null) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "{\"response\" : \"InstanceId required for update\"}");
        }

        EPOSDataModelEntity existingEntity = (EPOSDataModelEntity) dbapi.retrieve(obj.getInstanceId());
        if(existingEntity == null) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "{\"response\" : \"Entity not found\"}");
        }

        // Pre-fetch user's group roles once for permission check (optimization)
        final Map<String, String> userGroupRoles = user.getIsAdmin() ? null : getUserAcceptedGroupRoles(user);

        if(!checkUserPermissionsReadWrite(existingEntity, user, userGroupRoles)) {
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"The user can't manage this action\"}");
        }

        StatusType currentStatus = existingEntity.getStatus();
        StatusType newStatus = obj.getStatus() != null ? obj.getStatus() : StatusType.DRAFT;

        EPOSDataModelEntity entityToSave = (obj.getUid() == null) ? existingEntity : obj;

        entityToSave.setStatus(newStatus);
        entityToSave.setFileProvenance("backoffice");

        if (currentStatus == StatusType.PUBLISHED && newStatus == StatusType.PUBLISHED
                && (entityNames.equals(EntityNames.CATEGORYSCHEME) || entityNames.equals(EntityNames.CATEGORY)
                || entityNames.equals(EntityNames.ORGANIZATION) || entityNames.equals(EntityNames.PERSON))) {
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // DRAFT -> DRAFT: Check if same user or create new DRAFT
        if (currentStatus == StatusType.DRAFT && newStatus == StatusType.DRAFT) {

            entityToSave.setEditorId(user.getAuthIdentifier());

            // If same user, modify existing DRAFT
            if (user.getIsAdmin() || user.getAuthIdentifier().equals(existingEntity.getEditorId())) {
                LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
                return new ApiResponseMessage(ApiResponseMessage.OK, reference);
            }
            // Different user trying to modify someone else's DRAFT -> Create NEW DRAFT
            // This allows multiple users to have their own DRAFTs of the same entity
            entityToSave.setInstanceId(UUID.randomUUID().toString());
            entityToSave.setMetaId(existingEntity.getMetaId());
            entityToSave.setInstanceChangedId(existingEntity.getInstanceChangedId() != null ?
                    existingEntity.getInstanceChangedId() : existingEntity.getInstanceId());

            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);

            if(existingEntity.getGroups() != null) {
                for(String gid : existingEntity.getGroups()) {
                    UserGroupManagementAPI.addMetadataElementToGroup(reference.getMetaId(), gid);
                }
            }
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // PUBLISHED -> ARCHIVED/DISCARDED: Direct status change (no new version)
        if (currentStatus == StatusType.PUBLISHED && (newStatus == StatusType.ARCHIVED || newStatus == StatusType.DISCARDED)) {
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        if ((currentStatus == StatusType.DRAFT || currentStatus == StatusType.SUBMITTED) && newStatus == StatusType.DISCARDED) {
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // PUBLISHED -> any other modification: Create new DRAFT version
        if (currentStatus == StatusType.PUBLISHED && newStatus != StatusType.ARCHIVED && newStatus != StatusType.DISCARDED) {
            entityToSave.setInstanceId(UUID.randomUUID().toString());
            entityToSave.setMetaId(existingEntity.getMetaId());
            entityToSave.setStatus(StatusType.DRAFT);
            entityToSave.setInstanceChangedId(existingEntity.getInstanceId());
            entityToSave.setEditorId(user.getAuthIdentifier());

            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);

            if(existingEntity.getGroups() != null) {
                for(String gid : existingEntity.getGroups()) {
                    UserGroupManagementAPI.addMetadataElementToGroup(reference.getMetaId(), gid);
                }
            }
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // DRAFT -> SUBMITTED: Status change only
        if (currentStatus == StatusType.DRAFT && newStatus == StatusType.SUBMITTED) {
            // Only the owner or admin can submit
            //if (!user.getIsAdmin() && !user.getAuthIdentifier().equals(existingEntity.getEditorId())) {
            //    return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"Only the DRAFT owner or Admin can submit.\"}");
            //}
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);

            EPOSDataModelEntity entity = (EPOSDataModelEntity) dbapi.retrieve(reference.getInstanceId());
            EmailWrapper.wrapSubmitted(entity,user,entity.getMetaId(), entity.getInstanceId());

            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // SUBMITTED -> PUBLISHED: Status change + archive old PUBLISHED versions
        if (currentStatus == StatusType.SUBMITTED && newStatus == StatusType.PUBLISHED) {
            // Only admin or reviewer can publish
            //if (!user.getIsAdmin() && !hasReviewerRole(user, existingEntity)) {
            //    return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "{\"response\" : \"Only Admin or Reviewer can publish.\"}");
            //}
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            archiveOldPublishedVersions(dbapi, reference.getMetaId(), reference.getInstanceId());
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // ARCHIVED -> any: Not allowed (ARCHIVED entities are read-only)
        if (currentStatus == StatusType.ARCHIVED) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "{\"response\" : \"Cannot modify ARCHIVED entity. ARCHIVED entities are read-only.\"}");
        }

        return new ApiResponseMessage(ApiResponseMessage.ERROR, "{\"response\" : \"Invalid status transition from " + currentStatus + " to " + newStatus+"\"}");
    }

    private static void archiveOldPublishedVersions(AbstractAPI dbapi, String metaId, String currentInstanceId) {
        List<Object> allPublished = dbapi.retrieveAllWithStatus(StatusType.PUBLISHED);
        for (Object item : allPublished) {
            EPOSDataModelEntity entity = (EPOSDataModelEntity) item;
            if (entity.getMetaId().equals(metaId) && !entity.getInstanceId().equals(currentInstanceId)) {
                entity.setStatus(StatusType.ARCHIVED);
                dbapi.create(entity, null, null, null);
            }
        }
    }

    // ==================== PERMISSION HELPER METHODS ====================

    /**
     * Returns a map of groupId -> highest role for all ACCEPTED memberships of the user.
     * Single DB query, O(1) lookup afterwards.
     */
    private static Map<String, String> getUserAcceptedGroupRoles(User user) {
        Map<String, String> groupRoles = new HashMap<>();
        
        // Single DB query: get all memberships for this user
        List<MetadataGroupUser> allMemberships = getDbaccess().getOneFromDBBySpecificKeySimple(
                "authIdentifier.authIdentifier", user.getAuthIdentifier(), MetadataGroupUser.class);
        
        for (MetadataGroupUser membership : allMemberships) {
            // Only consider ACCEPTED memberships
            if (!RequestStatusType.ACCEPTED.name().equals(membership.getRequestStatus())) {
                continue;
            }
            
            String groupId = membership.getGroup().getId();
            String role = membership.getRole();
            
            // Keep highest role per group (in case of duplicates)
            String existingRole = groupRoles.get(groupId);
            if (existingRole == null || isHigherRole(role, existingRole)) {
                groupRoles.put(groupId, role);
            }
        }
        
        return groupRoles;
    }

    /**
     * Compares role priority: ADMIN > REVIEWER > EDITOR > VIEWER
     * @return true if newRole has higher priority than existingRole
     */
    private static boolean isHigherRole(String newRole, String existingRole) {
        return getRolePriority(newRole) > getRolePriority(existingRole);
    }

    /**
     * Returns the priority of a role. Higher number = higher priority.
     */
    private static int getRolePriority(String role) {
        if (RoleType.ADMIN.name().equals(role)) return 4;
        if (RoleType.REVIEWER.name().equals(role)) return 3;
        if (RoleType.EDITOR.name().equals(role)) return 2;
        if (RoleType.VIEWER.name().equals(role)) return 1;
        return 0;
    }

    /**
     * Returns the user's highest-privilege role in the entity's groups.
     * Uses pre-fetched userGroupRoles map for O(1) lookups.
     * Role priority: ADMIN > REVIEWER > EDITOR > VIEWER
     * Returns null if user has no ACCEPTED membership in any of the entity's groups (external user).
     */
    private static String getUserRoleInEntityGroups(EPOSDataModelEntity obj, Map<String, String> userGroupRoles) {
        if (obj.getGroups() == null || obj.getGroups().isEmpty()) {
            return null;
        }

        String highestRole = null;

        for (String groupId : obj.getGroups()) {
            String role = userGroupRoles.get(groupId);
            if (role == null) {
                continue; // User not a member of this group
            }

            // Return immediately for ADMIN (highest priority)
            if (RoleType.ADMIN.name().equals(role)) {
                return role;
            }

            // Update highestRole based on priority
            if (highestRole == null || isHigherRole(role, highestRole)) {
                highestRole = role;
            }
        }
        return highestRole;
    }

    /**
     * Checks if a user can CREATE/MODIFY an entity. Fetches user roles from DB.
     * For batch operations, use the overloaded version with pre-fetched userGroupRoles.
     */
    private static boolean checkUserPermissionsReadWrite(EPOSDataModelEntity obj, User user) {
        if (user.getIsAdmin()) {
            // Early return for backoffice admin (no need to fetch roles)
            if (obj.getStatus() == StatusType.ARCHIVED) {
                return false;
            }
            return true;
        }
        return checkUserPermissionsReadWrite(obj, user, getUserAcceptedGroupRoles(user));
    }

    /**
     * Checks if a user can CREATE/MODIFY an entity using pre-fetched user group roles.
     * This is the optimized version for batch operations.
     */
    private static boolean checkUserPermissionsReadWrite(EPOSDataModelEntity obj, User user, Map<String, String> userGroupRoles) {
        log.debug("checkUserPermissionsReadWrite - entity metaId: {}, instanceId: {}, status: {}, groups: {}", 
                obj.getMetaId(), obj.getInstanceId(), obj.getStatus(), obj.getGroups());
        log.debug("checkUserPermissionsReadWrite - user: {}, isAdmin: {}", 
                user.getAuthIdentifier(), user.getIsAdmin());

        // Backoffice admin has full access to everything (except creating ARCHIVED)
        if (user.getIsAdmin()) {
            // Even backoffice admins cannot create ARCHIVED entities directly
            if (obj.getStatus() == StatusType.ARCHIVED) {
                log.debug("checkUserPermissionsReadWrite - cannot create/modify ARCHIVED entities");
                return false;
            }
            log.debug("checkUserPermissionsReadWrite - backoffice admin, granting access");
            return true;
        }

        // Entities without groups are only accessible to backoffice admins
        if (obj.getGroups() == null || obj.getGroups().isEmpty()) {
            log.debug("checkUserPermissionsReadWrite - entity has no groups, returning false");
            return false;
        }

        // Get entity status (default to DRAFT if not set)
        StatusType status = obj.getStatus();
        if (status == null) {
            status = StatusType.DRAFT;
        }

        // No one can create ARCHIVED entities directly
        if (status == StatusType.ARCHIVED) {
            log.debug("checkUserPermissionsReadWrite - cannot create/modify ARCHIVED entities");
            return false;
        }

        // Get user's role in the entity's groups using pre-fetched map
        String userRole = getUserRoleInEntityGroups(obj, userGroupRoles);
        log.debug("checkUserPermissionsReadWrite - user role in entity groups: {}", userRole);

        // No membership in entity's groups = external user = no access
        if (userRole == null) {
            log.debug("checkUserPermissionsReadWrite - user has no role in entity groups (external user), denying access");
            return false;
        }

        // Group ADMIN has full access within their groups (except ARCHIVED handled above)
        if (RoleType.ADMIN.name().equals(userRole)) {
            log.debug("checkUserPermissionsReadWrite - group admin, granting access");
            return true;
        }

        // Check if user is the owner (for "self" permissions)
        boolean isOwner = user.getAuthIdentifier() != null && 
                          user.getAuthIdentifier().equals(obj.getEditorId());
        log.debug("checkUserPermissionsReadWrite - isOwner: {}", isOwner);

        // Apply create/write permissions based on role and status
        switch (status) {
            case DRAFT:
                // viewer: no, editor: all, reviewer: no
                if (RoleType.EDITOR.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadWrite - DRAFT, editor role, granting access");
                    return true;
                }
                log.debug("checkUserPermissionsReadWrite - DRAFT, role {} has no write access", userRole);
                return false;

            case SUBMITTED:
                // viewer: no, editor: self, reviewer: no
                if (RoleType.EDITOR.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadWrite - SUBMITTED, editor role, access based on ownership: {}", isOwner);
                    return isOwner;
                }
                log.debug("checkUserPermissionsReadWrite - SUBMITTED, role {} has no write access", userRole);
                return false;

            case PUBLISHED:
            case DISCARDED:
                // viewer: no, editor: no, reviewer: all
                if (RoleType.REVIEWER.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadWrite - {}, reviewer role, granting access", status);
                    return true;
                }
                log.debug("checkUserPermissionsReadWrite - {}, role {} has no write access", status, userRole);
                return false;

            default:
                log.debug("checkUserPermissionsReadWrite - unknown status {}, denying access", status);
                return false;
        }
    }

    /**
     * Checks if a user can VIEW an entity. Fetches user roles from DB.
     * For batch operations, use the overloaded version with pre-fetched userGroupRoles.
     */
    private static boolean checkUserPermissionsReadOnly(EPOSDataModelEntity obj, User user) {
        if (user.getIsAdmin()) {
            // Early return for backoffice admin (no need to fetch roles)
            return true;
        }
        return checkUserPermissionsReadOnly(obj, user, getUserAcceptedGroupRoles(user));
    }

    /**
     * Checks if a user can VIEW an entity using pre-fetched user group roles.
     * This is the optimized version for batch operations.
     */
    private static boolean checkUserPermissionsReadOnly(EPOSDataModelEntity obj, User user, Map<String, String> userGroupRoles) {
        log.debug("checkUserPermissionsReadOnly - entity metaId: {}, instanceId: {}, status: {}, groups: {}", 
                obj.getMetaId(), obj.getInstanceId(), obj.getStatus(), obj.getGroups());
        log.debug("checkUserPermissionsReadOnly - user: {}, isAdmin: {}", 
                user.getAuthIdentifier(), user.getIsAdmin());

        // Backoffice admin has full access to everything
        if (user.getIsAdmin()) {
            log.debug("checkUserPermissionsReadOnly - backoffice admin, granting access");
            return true;
        }

        // Entities without groups are only accessible to backoffice admins
        if (obj.getGroups() == null || obj.getGroups().isEmpty()) {
            log.debug("checkUserPermissionsReadOnly - entity has no groups, returning false");
            return false;
        }

        // Get entity status (default to DRAFT if not set)
        StatusType status = obj.getStatus();
        if (status == null) {
            status = StatusType.DRAFT;
        }

        // Get user's role in the entity's groups using pre-fetched map
        String userRole = getUserRoleInEntityGroups(obj, userGroupRoles);
        log.debug("checkUserPermissionsReadOnly - user role in entity groups: {}", userRole);

        // No membership in entity's groups = external user = no access
        if (userRole == null) {
            log.debug("checkUserPermissionsReadOnly - user has no role in entity groups (external user), denying access");
            return false;
        }

        // Group ADMIN has full access within their groups
        if (RoleType.ADMIN.name().equals(userRole)) {
            log.debug("checkUserPermissionsReadOnly - group admin, granting access");
            return true;
        }

        // Check if user is the owner (for "self" permissions)
        boolean isOwner = user.getAuthIdentifier() != null && 
                          user.getAuthIdentifier().equals(obj.getEditorId());
        log.debug("checkUserPermissionsReadOnly - isOwner: {}", isOwner);

        // Apply view permissions based on role and status
        switch (status) {
            case DRAFT:
                // viewer: no, editor: self, reviewer: no
                if (RoleType.EDITOR.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadOnly - DRAFT, editor role, access based on ownership: {}", isOwner);
                    return isOwner;
                }
                log.debug("checkUserPermissionsReadOnly - DRAFT, role {} has no access", userRole);
                return false;

            case SUBMITTED:
                // viewer: no, editor: self, reviewer: all
                if (RoleType.REVIEWER.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadOnly - SUBMITTED, reviewer role, granting access");
                    return true;
                }
                if (RoleType.EDITOR.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadOnly - SUBMITTED, editor role, access based on ownership: {}", isOwner);
                    return isOwner;
                }
                log.debug("checkUserPermissionsReadOnly - SUBMITTED, role {} has no access", userRole);
                return false;

            case PUBLISHED:
            case ARCHIVED:
                // viewer: all, editor: all, reviewer: all
                log.debug("checkUserPermissionsReadOnly - {}, any role with membership can view, granting access", status);
                return true;

            case DISCARDED:
                // viewer: no, editor: self, reviewer: all
                if (RoleType.REVIEWER.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadOnly - DISCARDED, reviewer role, granting access");
                    return true;
                }
                if (RoleType.EDITOR.name().equals(userRole)) {
                    log.debug("checkUserPermissionsReadOnly - DISCARDED, editor role, access based on ownership: {}", isOwner);
                    return isOwner;
                }
                log.debug("checkUserPermissionsReadOnly - DISCARDED, role {} has no access", userRole);
                return false;

            default:
                log.debug("checkUserPermissionsReadOnly - unknown status {}, denying access", status);
                return false;
        }
    }

    private static boolean hasReviewerRole(User user, EPOSDataModelEntity obj) {
        if(obj.getGroups() != null && !obj.getGroups().isEmpty()){
            for(String groupid : obj.getGroups()){
                Map<String, Object> filters = new HashMap<>();
                filters.put("group.id", groupid);
                filters.put("authIdentifier.authIdentifier", user.getAuthIdentifier());

                List<MetadataGroupUser> metadataGroupUserList = getDbaccess().getFromDBByUsingMultipleKeys(filters, MetadataGroupUser.class);

                for(MetadataGroupUser metadataGroupUser : metadataGroupUserList){
                    String role = metadataGroupUser.getRole();
                    String status = metadataGroupUser.getRequestStatus();
                    if((RoleType.ADMIN.name().equals(role) || RoleType.REVIEWER.name().equals(role))
                        && RequestStatusType.ACCEPTED.name().equals(status)){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns a list of group IDs where the user has write permission (EDITOR, REVIEWER, or ADMIN role with ACCEPTED status).
     * Fetches user roles from DB.
     */
    private static List<String> getUserWritableGroups(User user) {
        if(user.getIsAdmin()) {
            // Admins can write to all groups - but for entity creation, we still need at least one group
            // Return empty to trigger fallback to ALL group
            return new ArrayList<>();
        }
        return getUserWritableGroups(user, getUserAcceptedGroupRoles(user));
    }

    /**
     * Returns a list of group IDs where the user has write permission (EDITOR, REVIEWER, or ADMIN role with ACCEPTED status).
     * Uses pre-fetched userGroupRoles map for efficiency.
     */
    private static List<String> getUserWritableGroups(User user, Map<String, String> userGroupRoles) {
        List<String> writableGroups = new ArrayList<>();
        
        if(user.getIsAdmin()) {
            // Admins can write to all groups - but for entity creation, we still need at least one group
            // Return empty to trigger fallback to ALL group
            return writableGroups;
        }
        
        if(userGroupRoles == null) {
            return writableGroups;
        }
        
        // Filter groups where user has write permission (EDITOR, REVIEWER, or ADMIN)
        for(Map.Entry<String, String> entry : userGroupRoles.entrySet()) {
            String role = entry.getValue();
            if(RoleType.ADMIN.name().equals(role)
                    || RoleType.REVIEWER.name().equals(role)
                    || RoleType.EDITOR.name().equals(role)) {
                writableGroups.add(entry.getKey());
            }
        }
        
        return writableGroups;
    }

    public static ApiResponseMessage deleteEposDataModelEntity(String instance_id, User user, EntityNames entityNames, Class clazz) {
        EposDataModelDAO.getInstance().clearAllCaches();
        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());
        
        EPOSDataModelEntity existingEntity = (EPOSDataModelEntity) dbapi.retrieve(instance_id);
        if(existingEntity == null) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "Entity not found");
        }
        
        if(!checkUserPermissionsReadWrite(existingEntity, user)) {
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "The user can't delete this entity");
        }
        
        dbapi.delete(instance_id);
        return new ApiResponseMessage(ApiResponseMessage.OK, "Entity deleted successfully");
    }

	private static void updatePluginsForDraftDistribution(LinkedEntity newDataProductLinkedEntity, DataProduct oldDataProduct) {
		if (newDataProductLinkedEntity == null || oldDataProduct == null) {
			return;
		}

		// TODO: don't stop everything on single post fail, try the others
		try {
			DataProduct newDataProduct = (DataProduct) LinkedEntityAPI.retrieveFromLinkedEntity(newDataProductLinkedEntity);

			List<Distribution> oldDistributions = new ArrayList<>();
			List<Distribution> newDistributions = new ArrayList<>();

			for (var linkedEntity: newDataProduct.getDistribution()){
				Distribution distribution = (Distribution) LinkedEntityAPI.retrieveFromLinkedEntity(linkedEntity);
				if (distribution == null) {
					continue;
				}
				newDistributions.add(distribution);
			}
			for (var linkedEntity: oldDataProduct.getDistribution()){
				Distribution distribution = (Distribution) LinkedEntityAPI.retrieveFromLinkedEntity(linkedEntity);
				if (distribution == null) {
					continue;
				}
				oldDistributions.add(distribution);
			}


			// match the distributions from the old and new
			for (var newDistribution: newDistributions) {
				for (var oldDistribution: oldDistributions) {
					if (newDistribution.getMetaId().equals(oldDistribution.getMetaId())) {
						// they are the same, we have to update the new one
						try {
							String getUrl = "http://converter-service:8080/api/converter-service/v1/distributions/" + oldDistribution.getInstanceId();
							PluginResponse relations = restTemplate.getForObject(getUrl, PluginResponse.class);
							if (relations != null) {
								for (PluginResponse.PluginRelation relation : relations.getRelations()) {
									var rel = relation.getRelation();
									PluginResponse.Relation newRel = new PluginResponse.Relation(rel.getId(), rel.getInputFormat(), rel.getOutputFormat(), rel.getPluginId(), newDistribution.getInstanceId());
									restTemplate.postForObject("http://converter-service:8080/api/converter-service/v1/plugin-relations", newRel, Void.class);
								}
							}
						} catch (Exception e) {
							log.error("Failed to associate plugins for distribution {}: {}", newDistribution.getInstanceId(), e.getMessage(), e);
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("Failed to associate plugins for distribution {}: {}", newDataProductLinkedEntity.getInstanceId(), e.getMessage(), e);
		}
	}

	public static class PluginResponse {
		@JsonProperty("instance_id")
		private String instanceId;
		private List<PluginRelation> relations;
		
		public PluginResponse() {}
		
		public PluginResponse(String instanceId, List<PluginRelation> relations) {
			this.instanceId = instanceId;
			this.relations = relations;
		}
		
		public String getInstanceId() { return instanceId; }
		public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
		public List<PluginRelation> getRelations() { return relations; }
		public void setRelations(List<PluginRelation> relations) { this.relations = relations; }
		
		public static class PluginRelation {
			private Plugin plugin;
			private Relation relation;
			
			public PluginRelation() {}
			
			public PluginRelation(Plugin plugin, Relation relation) {
				this.plugin = plugin;
				this.relation = relation;
			}
			
			public Plugin getPlugin() { return plugin; }
			public void setPlugin(Plugin plugin) { this.plugin = plugin; }
			public Relation getRelation() { return relation; }
			public void setRelation(Relation relation) { this.relation = relation; }

			@Override
			public String toString() {
				return "PluginRelation [plugin=" + plugin + ", relation=" + relation + "]";
			}
		}
		
		public static class Plugin {
			private String arguments;
			private String description;
			private Boolean enabled;
			private String executable;
			private String id;
			private Boolean installed;
			private String name;
			private String repository;
			private String runtime;
			private String version;
			@JsonProperty("version_type")
			private String versionType;
			
			public Plugin() {}
			
			// Getters and Setters
			public String getArguments() { return arguments; }
			public void setArguments(String arguments) { this.arguments = arguments; }
			public String getDescription() { return description; }
			public void setDescription(String description) { this.description = description; }
			public Boolean getEnabled() { return enabled; }
			public void setEnabled(Boolean enabled) { this.enabled = enabled; }
			public String getExecutable() { return executable; }
			public void setExecutable(String executable) { this.executable = executable; }
			public String getId() { return id; }
			public void setId(String id) { this.id = id; }
			public Boolean getInstalled() { return installed; }
			public void setInstalled(Boolean installed) { this.installed = installed; }
			public String getName() { return name; }
			public void setName(String name) { this.name = name; }
			public String getRepository() { return repository; }
			public void setRepository(String repository) { this.repository = repository; }
			public String getRuntime() { return runtime; }
			public void setRuntime(String runtime) { this.runtime = runtime; }
			public String getVersion() { return version; }
			public void setVersion(String version) { this.version = version; }
			public String getVersionType() { return versionType; }
			public void setVersionType(String versionType) { this.versionType = versionType; }

			@Override
			public String toString() {
				return "Plugin [arguments=" + arguments + ", description=" + description + ", enabled=" + enabled + ", executable=" + executable + ", id=" + id + ", installed=" + installed + ", name=" + name + ", repository=" + repository + ", runtime=" + runtime + ", version=" + version + ", versionType=" + versionType + "]";
			}
		}
		
		public static class Relation {
			private String id;
			@JsonProperty("input_format")
			private String inputFormat;
			
			@JsonProperty("output_format")
			private String outputFormat;
			
			@JsonProperty("plugin_id")
			private String pluginId;
			
			@JsonProperty("relation_id")
			private String relationId;
			
			public Relation() {}
			
			public Relation(String id, String inputFormat, String outputFormat, String pluginId, String relationId) {
				this.id = id;
				this.inputFormat = inputFormat;
				this.outputFormat = outputFormat;
				this.pluginId = pluginId;
				this.relationId = relationId;
			}
			
			public String getId() { return id; }
			public void setId(String id) { this.id = id; }
			public String getInputFormat() { return inputFormat; }
			public void setInputFormat(String inputFormat) { this.inputFormat = inputFormat; }
			public String getOutputFormat() { return outputFormat; }
			public void setOutputFormat(String outputFormat) { this.outputFormat = outputFormat; }
			public String getPluginId() { return pluginId; }
			public void setPluginId(String pluginId) { this.pluginId = pluginId; }
			public String getRelationId() { return relationId; }
			public void setRelationId(String relationId) { this.relationId = relationId; }

			@Override
			public String toString() {
				return "Relation [id=" + id + ", inputFormat=" + inputFormat + ", outputFormat=" + outputFormat + ", pluginId=" + pluginId + ", relationId=" + relationId + "]";
			}
		}
	}
}
