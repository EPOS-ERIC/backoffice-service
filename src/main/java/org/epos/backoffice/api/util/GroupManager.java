package org.epos.backoffice.api.util;

import dao.EposDataModelDAO;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.User;
import usermanagementapis.UserGroupManagementAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GroupManager {


	public static ApiResponseMessage getGroup(String instance_id, User user, Boolean available_section) {

		if (instance_id == null)
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank");

		List<Group> groupList;
		if (instance_id.equals("all")) {
			groupList = UserGroupManagementAPI.retrieveAllGroups();
		} else {
			Group tempGroup = Optional.ofNullable(UserGroupManagementAPI.retrieveGroupById(instance_id)).orElse(null);
			groupList = tempGroup!=null? List.of(tempGroup) : new ArrayList<>();
		}


		List<Group> groupStream = groupList.stream()
				.filter(x -> x.getId() != null && !x.getId().isEmpty()).collect(Collectors.toList());

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

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't create groups");

		if(UserGroupManagementAPI.createGroup(inputGroup)){

			return new ApiResponseMessage(ApiResponseMessage.OK, "Group created successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't create a group");
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage updateGroup(Group inputGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't update groups");

		if(UserGroupManagementAPI.createGroup(inputGroup)){

			return new ApiResponseMessage(ApiResponseMessage.OK, "Group updated successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update other group");
	}

	public static ApiResponseMessage deleteGroup(String instance_id, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't delete groups");

		if(UserGroupManagementAPI.deleteGroup(instance_id)) {

			return new ApiResponseMessage(ApiResponseMessage.OK, "Group deleted successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete other group");
	}

	public static ApiResponseMessage addEntityToGroup(AddEntityToGroupBean entityGroup, User user) {

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't add entities to groups");

		Boolean result = UserGroupManagementAPI.addMetadataElementToGroup(
				entityGroup.getMetaid(),
				entityGroup.getGroupid());

		if(result!=null && result)
			return new ApiResponseMessage(ApiResponseMessage.OK, "Entity added successfully to group");

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "Error on adding the entity to the group");
	}

    public static ApiResponseMessage removeEntityFromGroup(AddEntityToGroupBean addEntityToGroupBean, User user) {

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "You can't remove entities from groups");

		Boolean result = UserGroupManagementAPI.removeMetadataElementFromGroup(
				addEntityToGroupBean.getMetaid(),
				addEntityToGroupBean.getGroupid());

		if(result!=null && result)
			return new ApiResponseMessage(ApiResponseMessage.OK, "Entity remove successfully from group");

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "Error on remove the entity from the group");
    }
}
