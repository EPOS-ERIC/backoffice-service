package org.epos.backoffice.api.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.UserGroup;

import abstractapis.AbstractAPI;
import dao.EposDataModelDAO;
import metadataapis.EntityNames;
import model.RoleType;
import model.StatusType;
import usermanagementapis.UserGroupManagementAPI;

public class EPOSDataModelManager {

    public static ApiResponseMessage getEPOSDataModelEposDataModelEntity(String meta_id, String instance_id, User user, EntityNames entityNames, Class clazz) {

        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());
        if (meta_id == null)
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "The [meta_id] field can't be left blank");
        if(instance_id == null) {
            instance_id = "all";
        }

        if((entityNames.equals(EntityNames.PERSON)
                || entityNames.equals(EntityNames.ORGANIZATION)
                || entityNames.equals(EntityNames.CONTACTPOINT)) && !user.getIsAdmin()){
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "A user which is not Admin can't access PERSON/ORGANIZATION/CONTACTPOINT entities due to privacy settings");
        }

        List<EPOSDataModelEntity> list;
        if (meta_id.equals("all")) {
            list = dbapi.retrieveAll();
        } else {
            if(instance_id.equals("all")) {
                list = dbapi.retrieveAll();
                list = list.stream()
                        .filter(elem -> elem.getMetaId().equals(meta_id))
                        .collect(Collectors.toList());
            } else {
                list = new ArrayList<>();
                EPOSDataModelEntity entity = (EPOSDataModelEntity) dbapi.retrieve(instance_id);
                if(entity!=null) list.add(entity);
            }
        }

        List<String> userGroups = UserGroupManagementAPI.retrieveUserById(user.getAuthIdentifier()).getGroups().stream().map(UserGroup::getGroupId).collect(Collectors.toList());

        Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
        if(userGroups.isEmpty() && allGroup != null) {
            userGroups.add(allGroup.getId());
        }

        List<EPOSDataModelEntity> revertedList = new ArrayList<>();
        list.forEach(e -> {
            if(UserGroupManagementAPI.checkIfMetaIdAndUserIdAreInSameGroup(e.getMetaId(), user.getAuthIdentifier())){

                e.setGroups(UserGroupManagementAPI.retrieveShortGroupsFromMetaId(e.getMetaId()));
                revertedList.add(0, e);
            }
        });

        if (revertedList.isEmpty())
            return new ApiResponseMessage(ApiResponseMessage.OK, new ArrayList<EPOSDataModelEntity>());

        return new ApiResponseMessage(ApiResponseMessage.OK, revertedList);
    }

    public static ApiResponseMessage createEposDataModelEntity(EPOSDataModelEntity obj, User user, EntityNames entityNames, Class clazz) {

        EposDataModelDAO.getInstance().clearAllCaches();

        if(!checkUserPermissions(obj, user)) {
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "The user can't manage this action");
        }

        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());

        if(obj.getInstanceId() != null)
            obj.setInstanceChangedId(obj.getInstanceId());

        obj.setStatus(obj.getStatus()==null? StatusType.DRAFT : obj.getStatus());
        obj.setEditorId(user.getAuthIdentifier());
        obj.setFileProvenance("backoffice");

        Group allGroup = UserGroupManagementAPI.retrieveGroupByName("ALL");
        if((obj.getGroups()==null || obj.getGroups().isEmpty()) && allGroup != null) {
            obj.setGroups(List.of(allGroup.getId()));
        }

        LinkedEntity reference = dbapi.create(obj, null, null, null);

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
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "InstanceId required for update");
        }

        EPOSDataModelEntity existingEntity = (EPOSDataModelEntity) dbapi.retrieve(obj.getInstanceId());
        if(existingEntity == null) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "Entity not found");
        }

        if(!checkUserPermissions(existingEntity, user)) {
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "The user can't manage this action");
        }

        StatusType currentStatus = existingEntity.getStatus();
        StatusType newStatus = obj.getStatus() != null ? obj.getStatus() : StatusType.DRAFT;

        EPOSDataModelEntity entityToSave = (obj.getUid() == null) ? existingEntity : obj;

        entityToSave.setStatus(newStatus);
        entityToSave.setEditorId(user.getAuthIdentifier());
        entityToSave.setFileProvenance("backoffice");

        if (currentStatus == StatusType.DRAFT && newStatus == StatusType.DRAFT) {
            if (!user.getIsAdmin() && !existingEntity.getEditorId().equals(user.getAuthIdentifier())) {
                return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "Only the creator or an Admin can modify this DRAFT.");
            }
            // Usa entityToSave (che ha i dati completi)
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        if (currentStatus == StatusType.PUBLISHED) {

            entityToSave.setInstanceId(UUID.randomUUID().toString());
            entityToSave.setMetaId(existingEntity.getMetaId());
            entityToSave.setStatus(StatusType.DRAFT);
            entityToSave.setInstanceChangedId(existingEntity.getInstanceId());

            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);

            if(existingEntity.getGroups() != null) {
                for(String gid : existingEntity.getGroups()) {
                    UserGroupManagementAPI.addMetadataElementToGroup(reference.getMetaId(), gid);
                }
            }
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        if ((currentStatus == StatusType.DRAFT && newStatus == StatusType.SUBMITTED) ||
                (currentStatus == StatusType.SUBMITTED && newStatus == StatusType.PUBLISHED)) {

            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);

            if (newStatus == StatusType.PUBLISHED) {
                archiveOldPublishedVersions(dbapi, reference.getMetaId(), reference.getInstanceId());
            }
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        return new ApiResponseMessage(ApiResponseMessage.ERROR, "Invalid status transition from " + currentStatus + " to " + newStatus);
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

    private static boolean checkUserPermissions(EPOSDataModelEntity obj, User user) {
        if(user.getIsAdmin()) return true;
        if(obj.getGroups() != null && !obj.getGroups().isEmpty()){
            for(String groupid : obj.getGroups()){
                for(UserGroup group1 : user.getGroups()){
                    if(groupid.equals(group1.getGroupId())
                            && (group1.getRole().equals(RoleType.ADMIN)
                            || group1.getRole().equals(RoleType.REVIEWER)
                            || group1.getRole().equals(RoleType.EDITOR))){
                        return true;
                    }
                }
            }
            return false;
        }
        return true;
    }

    public static boolean deleteEposDataModelEntity(String instance_id, User user, EntityNames entityNames, Class clazz) {
        EposDataModelDAO.getInstance().clearAllCaches();
        AbstractAPI dbapi = AbstractAPI.retrieveAPI(entityNames.name());
        dbapi.delete(instance_id);
        return true;
    }
}