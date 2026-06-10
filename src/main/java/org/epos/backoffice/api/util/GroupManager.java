package org.epos.backoffice.api.util;

import dao.EposDataModelDAO;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import usermanagementapis.UserGroupManagementAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GroupManager {

	private static final Logger log = LoggerFactory.getLogger(GroupManager.class);

	public static ApiResponseMessage getGroup(String instance_id, User user, Boolean available_section) {

		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;

		if (instance_id == null) {
			log.warn("Group read rejected: missing instanceId requester={} availableSection={}", requesterId, available_section);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank");
		}

		log.debug("Group read request requester={} instanceId={} availableSection={}", requesterId, instance_id, available_section);

		List<Group> groupList;
		if (instance_id.equals("all")) {
			List<Group> retrievedGroups = UserGroupManagementAPI.retrieveAllGroups();
			groupList = retrievedGroups != null ? retrievedGroups : new ArrayList<>();
			if (retrievedGroups == null) {
				log.warn("Group read returned null list requester={} target=all", requesterId);
			}
		} else {
			Group tempGroup = Optional.ofNullable(UserGroupManagementAPI.retrieveGroupById(instance_id)).orElse(null);
			groupList = tempGroup != null ? List.of(tempGroup) : new ArrayList<>();
			if (tempGroup == null) {
				log.warn("Group read target not found requester={} targetGroupId={}", requesterId, instance_id);
			}
		}

		List<Group> groupStream = groupList.stream()
				.filter(x -> x.getId() != null && !x.getId().isEmpty()).collect(Collectors.toList());

		log.debug("Group read result requester={} instanceId={} count={}", requesterId, instance_id, groupStream.size());

		if (groupStream.isEmpty())
			return new ApiResponseMessage(ApiResponseMessage.OK, false, true, new ArrayList<Group>());

		return new ApiResponseMessage(ApiResponseMessage.OK, false, true, groupStream);
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage createGroup(Group inputGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group create request requester={} groupId={} groupName={}", requesterId,
				inputGroup != null ? inputGroup.getId() : null,
				inputGroup != null ? inputGroup.getName() : null);
		if (user == null) {
			log.warn("Group create rejected: missing session user groupId={}", inputGroup != null ? inputGroup.getId() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group create denied requester={} groupId={}", requesterId, inputGroup != null ? inputGroup.getId() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't create groups");
		}

		if(UserGroupManagementAPI.createGroup(inputGroup)){
			log.info("Group created groupId={} requestedBy={}", inputGroup != null ? inputGroup.getId() : null, requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "Group created successfully");
		}

		log.error("Group create failed groupId={} requestedBy={}", inputGroup != null ? inputGroup.getId() : null, requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't create a group");
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage updateGroup(Group inputGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group update request requester={} groupId={} groupName={}", requesterId,
				inputGroup != null ? inputGroup.getId() : null,
				inputGroup != null ? inputGroup.getName() : null);
		if (user == null) {
			log.warn("Group update rejected: missing session user groupId={}", inputGroup != null ? inputGroup.getId() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group update denied requester={} groupId={}", requesterId, inputGroup != null ? inputGroup.getId() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't update groups");
		}

		if(UserGroupManagementAPI.createGroup(inputGroup)){
			log.info("Group updated groupId={} requestedBy={}", inputGroup != null ? inputGroup.getId() : null, requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "Group updated successfully");
		}

		log.error("Group update failed groupId={} requestedBy={}", inputGroup != null ? inputGroup.getId() : null, requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update other group");
	}

	public static ApiResponseMessage deleteGroup(String instance_id, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group delete request requester={} groupId={}", requesterId, instance_id);
		if (user == null) {
			log.warn("Group delete rejected: missing session user groupId={}", instance_id);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group delete denied requester={} groupId={}", requesterId, instance_id);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't delete groups");
		}

		if(UserGroupManagementAPI.deleteGroup(instance_id)) {
			log.info("Group deleted groupId={} requestedBy={}", instance_id, requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "Group deleted successfully");
		}

		log.error("Group delete failed groupId={} requestedBy={}", instance_id, requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete other group");
	}

	public static ApiResponseMessage addEntityToGroup(AddEntityToGroupBean entityGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group entity add request requester={} metaId={} groupId={}", requesterId,
				entityGroup != null ? entityGroup.getMetaid() : null,
				entityGroup != null ? entityGroup.getGroupid() : null);
		if (user == null) {
			log.warn("Group entity add rejected: missing session user metaId={} groupId={}",
					entityGroup != null ? entityGroup.getMetaid() : null,
					entityGroup != null ? entityGroup.getGroupid() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group entity add denied requester={} metaId={} groupId={}", requesterId,
					entityGroup != null ? entityGroup.getMetaid() : null,
					entityGroup != null ? entityGroup.getGroupid() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't add entities to groups");
		}

		Boolean result = UserGroupManagementAPI.addMetadataElementToGroup(
				entityGroup.getMetaid(),
				entityGroup.getGroupid());

		if(result!=null && result) {
			log.info("Group entity added metaId={} groupId={} requestedBy={}",
					entityGroup != null ? entityGroup.getMetaid() : null,
					entityGroup != null ? entityGroup.getGroupid() : null,
					requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "Entity added successfully to group");
		}

		log.error("Group entity add failed metaId={} groupId={} requestedBy={}",
				entityGroup != null ? entityGroup.getMetaid() : null,
				entityGroup != null ? entityGroup.getGroupid() : null,
				requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "Error on adding the entity to the group");
	}

    public static ApiResponseMessage removeEntityFromGroup(AddEntityToGroupBean addEntityToGroupBean, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group entity remove request requester={} metaId={} groupId={}", requesterId,
				addEntityToGroupBean != null ? addEntityToGroupBean.getMetaid() : null,
				addEntityToGroupBean != null ? addEntityToGroupBean.getGroupid() : null);
		if (user == null) {
			log.warn("Group entity remove rejected: missing session user metaId={} groupId={}",
					addEntityToGroupBean != null ? addEntityToGroupBean.getMetaid() : null,
					addEntityToGroupBean != null ? addEntityToGroupBean.getGroupid() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group entity remove denied requester={} metaId={} groupId={}", requesterId,
					addEntityToGroupBean != null ? addEntityToGroupBean.getMetaid() : null,
					addEntityToGroupBean != null ? addEntityToGroupBean.getGroupid() : null);
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't remove entities from groups");
		}

		Boolean result = UserGroupManagementAPI.removeMetadataElementFromGroup(
				addEntityToGroupBean.getMetaid(),
				addEntityToGroupBean.getGroupid());

		if(result!=null && result) {
			log.info("Group entity removed metaId={} groupId={} requestedBy={}",
					addEntityToGroupBean != null ? addEntityToGroupBean.getMetaid() : null,
					addEntityToGroupBean != null ? addEntityToGroupBean.getGroupid() : null,
					requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "Entity remove successfully from group");
		}

		log.error("Group entity remove failed metaId={} groupId={} requestedBy={}",
				addEntityToGroupBean != null ? addEntityToGroupBean.getMetaid() : null,
				addEntityToGroupBean != null ? addEntityToGroupBean.getGroupid() : null,
				requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "Error on remove the entity from the group");
    }
}
