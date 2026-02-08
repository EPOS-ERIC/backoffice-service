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
            list = list.stream()
                    .filter(elem -> elem.getMetaId().equals(meta_id))
                    .filter(elem -> checkUserPermissionsReadOnly(elem, user))
                    .collect(Collectors.toList());
        } else {
            if(instance_id.equals("all")) {
                list = dbapi.retrieveAll();
                list = list.stream()
                        .filter(elem -> elem.getMetaId().equals(meta_id))
                        .filter(elem -> checkUserPermissionsReadOnly(elem, user))
                        .collect(Collectors.toList());
            } else {
                list = new ArrayList<>();
                EPOSDataModelEntity entity = (EPOSDataModelEntity) dbapi.retrieve(instance_id);
                if(entity!=null) list.add(entity);
                list = list.stream()
                        .filter(elem -> elem.getMetaId().equals(meta_id))
                        .filter(elem -> checkUserPermissionsReadOnly(elem, user))
                        .collect(Collectors.toList());
            }
        }

        /*List<String> userGroups = UserGroupManagementAPI.retrieveUserById(user.getAuthIdentifier()).getGroups().stream().map(UserGroup::getGroupId).collect(Collectors.toList());

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

        return new ApiResponseMessage(ApiResponseMessage.OK, revertedList);*/
        if (list.isEmpty())
            return new ApiResponseMessage(ApiResponseMessage.OK, new ArrayList<EPOSDataModelEntity>());

        return new ApiResponseMessage(ApiResponseMessage.OK, list);
    }

    public static ApiResponseMessage createEposDataModelEntity(EPOSDataModelEntity obj, User user, EntityNames entityNames, Class clazz) {

        EposDataModelDAO.getInstance().clearAllCaches();

        if(!checkUserPermissionsReadWrite(obj, user)) {
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
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "InstanceId required for update");
        }

        EPOSDataModelEntity existingEntity = (EPOSDataModelEntity) dbapi.retrieve(obj.getInstanceId());
        if(existingEntity == null) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "Entity not found");
        }

        if(!checkUserPermissionsReadWrite(existingEntity, user)) {
            return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "The user can't manage this action");
        }

        StatusType currentStatus = existingEntity.getStatus();
        StatusType newStatus = obj.getStatus() != null ? obj.getStatus() : StatusType.DRAFT;

        EPOSDataModelEntity entityToSave = (obj.getUid() == null) ? existingEntity : obj;

        entityToSave.setStatus(newStatus);
        entityToSave.setEditorId(user.getAuthIdentifier());
        entityToSave.setFileProvenance("backoffice");

        // DRAFT -> DRAFT: Check if same user or create new DRAFT
        if (currentStatus == StatusType.DRAFT && newStatus == StatusType.DRAFT) {
            // If same user, modify existing DRAFT
            if (user.getIsAdmin() || existingEntity.getEditorId().equals(user.getAuthIdentifier())) {
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
        if (currentStatus == StatusType.PUBLISHED && (newStatus != StatusType.ARCHIVED || newStatus != StatusType.DISCARDED)) {
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

        // DRAFT -> SUBMITTED: Status change only
        if (currentStatus == StatusType.DRAFT && newStatus == StatusType.SUBMITTED) {
            // Only the owner or admin can submit
            if (!user.getIsAdmin() && !existingEntity.getEditorId().equals(user.getAuthIdentifier())) {
                return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "Only the DRAFT owner or Admin can submit.");
            }
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // SUBMITTED -> PUBLISHED: Status change + archive old PUBLISHED versions
        if (currentStatus == StatusType.SUBMITTED && newStatus == StatusType.PUBLISHED) {
            LinkedEntity reference = dbapi.create(entityToSave, null, null, null);
            archiveOldPublishedVersions(dbapi, reference.getMetaId(), reference.getInstanceId());
            return new ApiResponseMessage(ApiResponseMessage.OK, reference);
        }

        // ARCHIVED -> any: Not allowed (ARCHIVED entities are read-only)
        if (currentStatus == StatusType.ARCHIVED) {
            return new ApiResponseMessage(ApiResponseMessage.ERROR, "Cannot modify ARCHIVED entity. ARCHIVED entities are read-only.");
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

    private static boolean checkUserPermissionsReadWrite(EPOSDataModelEntity obj, User user) {
        if(user.getIsAdmin()) return true;

        if(obj.getGroups() != null && !obj.getGroups().isEmpty()){
            for(String groupid : obj.getGroups()){

                Map<String, Object> filters = new HashMap<>();
                filters.put("group.id", groupid);
                filters.put("authIdentifier.authIdentifier", user.getAuthIdentifier());

                List<MetadataGroupUser> metadataGroupUserList = getDbaccess().getFromDBByUsingMultipleKeys(filters,MetadataGroupUser.class);

                System.out.println("metadataGroupUserList RW: "+metadataGroupUserList);
                if(!metadataGroupUserList.isEmpty()){
                        if((metadataGroupUserList.get(0).getRole().equals(RoleType.ADMIN)
                                || metadataGroupUserList.get(0).getRole().equals(RoleType.REVIEWER)
                                || metadataGroupUserList.get(0).getRole().equals(RoleType.EDITOR))
                            && metadataGroupUserList.get(0).getRequestStatus().equals(RequestStatusType.ACCEPTED)){
                            return true;
                        }
                }
            }
            return false;
        }
        return true;
    }

    private static boolean checkUserPermissionsReadOnly(EPOSDataModelEntity obj, User user) {
        if(user.getIsAdmin()) return true;

        if(obj.getGroups() != null && !obj.getGroups().isEmpty()){
            for(String groupid : obj.getGroups()){
                Map<String, Object> filters = new HashMap<>();
                filters.put("group.id", groupid);
                filters.put("authIdentifier.authIdentifier", user.getAuthIdentifier());

                List<MetadataGroupUser> metadataGroupUserList = getDbaccess().getFromDBByUsingMultipleKeys(filters,MetadataGroupUser.class);

                System.out.println("metadataGroupUserList RO: "+metadataGroupUserList);
                if(!metadataGroupUserList.isEmpty()){
                    if(metadataGroupUserList.get(0).getRequestStatus().equals(RequestStatusType.ACCEPTED)){
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
