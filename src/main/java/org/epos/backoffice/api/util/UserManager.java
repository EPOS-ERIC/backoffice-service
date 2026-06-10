package org.epos.backoffice.api.util;


import java.util.*;
import java.util.stream.Collectors;

import dao.EposDataModelDAO;
import model.RequestStatusType;
import model.RoleType;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.UserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import usermanagementapis.UserGroupManagementAPI;

public class UserManager {

	private static final Logger log = LoggerFactory.getLogger(UserManager.class);


	public static ApiResponseMessage getUser(String instance_id, User user, Boolean available_section) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;

		if (instance_id == null) {
			log.warn("User read rejected: missing instanceId requester={} availableSection={}", requesterId, available_section);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank");
		}

		log.debug("User read request requester={} instanceId={} availableSection={}", requesterId, instance_id, available_section);

		List<User> personList;
		if(instance_id.equals("self")){
			if (requesterId == null) {
				log.warn("User read rejected: self lookup without session user");
				return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
			}
			User tempUser = UserGroupManagementAPI.retrieveUserById(user.getAuthIdentifier());
			personList = tempUser!=null? List.of(tempUser) : new ArrayList<>();
			if (tempUser == null) {
				log.warn("User read target not found requester={} target=self", requesterId);
			}
		}
		else if(instance_id.equals("all")){
			List<User> retrievedUsers = UserGroupManagementAPI.retrieveAllUsers();
			personList = retrievedUsers != null ? retrievedUsers : new ArrayList<>();
			if (retrievedUsers == null) {
				log.warn("User read returned null list requester={} target=all", requesterId);
			}
		} else {
			User tempUser = UserGroupManagementAPI.retrieveUserById(instance_id);
			personList = tempUser!=null? List.of(tempUser) : new ArrayList<>();
			if (tempUser == null) {
				log.warn("User read target not found requester={} targetUserId={}", requesterId, instance_id);
			}
		}

		List<User> userStream = personList.stream()
				.filter(x -> x.getAuthIdentifier() != null && !x.getAuthIdentifier().isEmpty()).collect(Collectors.toList());

		EposDataModelDAO.getInstance().clearAllCaches();
		log.debug("User read result requester={} instanceId={} count={}", requesterId, instance_id, userStream.size());

		if (userStream.isEmpty())
			return new ApiResponseMessage(ApiResponseMessage.OK, true, new ArrayList<User>());

		return new ApiResponseMessage(ApiResponseMessage.OK, true, userStream);
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage createUser(User inputUser, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		boolean requesterCanManageUsers = isExistingAdmin(user) || isBootstrapAdmin(user);
		String requesterId = user != null ? user.getAuthIdentifier() : null;

		if(!requesterCanManageUsers) {
			log.warn("User create denied requester={} targetUserId={}", requesterId, inputUser != null ? inputUser.getAuthIdentifier() : null);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't register other user");
		}

		inputUser.setFirstName(inputUser.getFirstName() == null ? user.getFirstName() : inputUser.getFirstName());
		inputUser.setLastName(inputUser.getLastName() == null ? user.getLastName() : inputUser.getLastName());
		inputUser.setEmail(inputUser.getEmail() == null ? user.getEmail() : inputUser.getEmail());
		inputUser.setAuthIdentifier(inputUser.getAuthIdentifier() == null ? user.getAuthIdentifier() : inputUser.getAuthIdentifier());
		inputUser.setIsAdmin(Boolean.TRUE.equals(inputUser.getIsAdmin()) && requesterCanManageUsers);

		if (UserGroupManagementAPI.createUser(inputUser)) {
			EposDataModelDAO.getInstance().clearAllCaches();
			log.info("User created targetUserId={} requestedBy={}", inputUser.getAuthIdentifier(), requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "User created successfully");
		}

		log.error("User create failed targetUserId={} requestedBy={}", inputUser.getAuthIdentifier(), requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't register other user");
	}

	public static ApiResponseMessage addUserToGroup(AddUserToGroupBean userGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group membership create request requester={} targetUserId={} groupId={} role={} status={}",
				requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getRole(), userGroup.getStatusType());
		if (user == null) {
			log.warn("Group membership create rejected: missing session user targetUserId={} groupId={}",
					userGroup.getUserid(), userGroup.getGroupid());
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}

		boolean acceptedStatus = RequestStatusType.ACCEPTED.name().equals(userGroup.getStatusType());
		boolean pendingStatus = RequestStatusType.PENDING.name().equals(userGroup.getStatusType());

		if((user == null || !Boolean.TRUE.equals(user.getIsAdmin())) && acceptedStatus) {
			log.warn("Group membership create denied requester={} targetUserId={} groupId={} status={}",
					requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getStatusType());
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't add users to groups");
		}

		Boolean result;
		try {
			result = UserGroupManagementAPI.addUserToGroup(
					userGroup.getGroupid(),
					userGroup.getUserid(),
					RoleType.valueOf(userGroup.getRole()),
					RequestStatusType.valueOf(userGroup.getStatusType()));
		} catch (IllegalArgumentException e) {
			log.error("Group membership create rejected: invalid role/status requester={} targetUserId={} groupId={} role={} status={}",
					requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getRole(), userGroup.getStatusType(), e);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "Invalid role or status");
		}

		EposDataModelDAO.getInstance().clearAllCaches();

		if(result!=null && result) {
			if(user != null && !Boolean.TRUE.equals(user.getIsAdmin()) && pendingStatus) {
				EmailWrapper.wrapGroupAccessRequest(userGroup, user);
			}
			log.info("Group membership created targetUserId={} groupId={} requestedBy={}",
					userGroup.getUserid(), userGroup.getGroupid(), requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "User added successfully");
		}

		log.error("Group membership create failed targetUserId={} groupId={} requestedBy={}",
				userGroup.getUserid(), userGroup.getGroupid(), requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't add the user to group");
	}

	public static ApiResponseMessage updateUserToGroup(AddUserToGroupBean userGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group membership update request requester={} targetUserId={} groupId={} role={} status={}",
				requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getRole(), userGroup.getStatusType());
		if (user == null) {
			log.warn("Group membership update rejected: missing session user targetUserId={} groupId={}",
					userGroup.getUserid(), userGroup.getGroupid());
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}


		User userRetrieved = UserGroupManagementAPI.retrieveUserById(userGroup.getUserid());
		if(userRetrieved==null) {
			log.warn("Group membership update rejected: target user not found targetUserId={} groupId={} requestedBy={}",
					userGroup.getUserid(), userGroup.getGroupid(), requesterId);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "User not found");
		}
		String previousRequestStatus = getGroupRequestStatus(userGroup.getGroupid(), userGroup.getUserid());
		boolean exists = false;
		List<UserGroup> userGroups = userRetrieved.getGroups() != null ? userRetrieved.getGroups() : Collections.emptyList();
		if (userRetrieved.getGroups() == null) {
			log.warn("Group membership update target has no groups targetUserId={} groupId={} requestedBy={}",
					userGroup.getUserid(), userGroup.getGroupid(), requesterId);
		}
		for(UserGroup userGroup1 : userGroups) {
			if(userGroup1.getGroupId().equals(userGroup.getGroupid())) exists = true;
		}

		if(!exists) return addUserToGroup(userGroup, user);

		if((user == null || !Boolean.TRUE.equals(user.getIsAdmin())) && RequestStatusType.ACCEPTED.name().equals(userGroup.getStatusType())) {
			log.warn("Group membership update denied requester={} targetUserId={} groupId={} status={}",
					requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getStatusType());
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update users to groups");
		}

		Boolean result;
		try {
			result = UserGroupManagementAPI.updateUserInGroup(
					userGroup.getGroupid(),
					userGroup.getUserid(),
					RoleType.valueOf(userGroup.getRole()),
					RequestStatusType.valueOf(userGroup.getStatusType()));
		} catch (IllegalArgumentException e) {
			log.error("Group membership update rejected: invalid role/status requester={} targetUserId={} groupId={} role={} status={}",
					requesterId, userGroup.getUserid(), userGroup.getGroupid(), userGroup.getRole(), userGroup.getStatusType(), e);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "Invalid role or status");
		}

		EposDataModelDAO.getInstance().clearAllCaches();

		if(result!=null && result) {
			if(shouldNotifyAcceptedGroupAccess(userGroup, previousRequestStatus)) {
				EmailWrapper.wrapGroupAccessAccepted(userGroup, userRetrieved, user);
			}
			log.info("Group membership updated targetUserId={} groupId={} requestedBy={}",
					userGroup.getUserid(), userGroup.getGroupid(), requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "User updated successfully");
		}

		log.error("Group membership update failed targetUserId={} groupId={} requestedBy={}",
				userGroup.getUserid(), userGroup.getGroupid(), requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update the user to group");
	}

	static boolean shouldNotifyAcceptedGroupAccess(AddUserToGroupBean userGroup, String previousRequestStatus) {
		if(userGroup == null) return false;
		if(!RequestStatusType.ACCEPTED.name().equals(userGroup.getStatusType())) return false;

		return !RequestStatusType.ACCEPTED.name().equals(previousRequestStatus);
	}

	private static String getGroupRequestStatus(String groupId, String userId) {
		if(groupId == null || userId == null) return null;

		org.epos.eposdatamodel.Group group = UserGroupManagementAPI.retrieveGroupById(groupId);
		if(group == null || group.getUsers() == null) return null;

		for(Map<String, String> groupUser : group.getUsers()) {
			if(groupUser != null && userId.equals(groupUser.get("userId"))) {
				return groupUser.get("requestStatus");
			}
		}

		return null;
	}


	public static ApiResponseMessage removeUserFromGroup(RemoveUserFromGroupBean removeUserFromGroupBean, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("Group membership removal request requester={} targetUserId={} groupId={}",
				requesterId, removeUserFromGroupBean.getUserid(), removeUserFromGroupBean.getGroupid());
		if (user == null) {
			log.warn("Group membership removal rejected: missing session user targetUserId={} groupId={}",
					removeUserFromGroupBean.getUserid(), removeUserFromGroupBean.getGroupid());
			return new ApiResponseMessage(ApiResponseMessage.UNAUTHORIZED, "User not found in session");
		}
		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("Group membership removal denied requester={} targetUserId={} groupId={}",
					requesterId, removeUserFromGroupBean.getUserid(), removeUserFromGroupBean.getGroupid());
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't remove users from groups");
		}

		Boolean result = UserGroupManagementAPI.removeUserFromGroup(
				removeUserFromGroupBean.getGroupid(),
				removeUserFromGroupBean.getUserid());

		// Clear caches AFTER the operation to ensure fresh data on subsequent reads
		EposDataModelDAO.getInstance().clearAllCaches();

		if(result!=null && result) {
			log.info("Group membership removed targetUserId={} groupId={} requestedBy={}",
					removeUserFromGroupBean.getUserid(), removeUserFromGroupBean.getGroupid(), requesterId);
			return new ApiResponseMessage(ApiResponseMessage.OK, "User removed successfully from group");
		}

		log.error("Group membership removal failed targetUserId={} groupId={} requestedBy={}",
				removeUserFromGroupBean.getUserid(), removeUserFromGroupBean.getGroupid(), requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't remove the user from group");

	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage updateUser(User inputUser, User user) {

		EposDataModelDAO.getInstance().clearAllCaches();
		boolean requesterCanManageUsers = isExistingAdmin(user) || isBootstrapAdmin(user);
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("User update request requester={} targetUserId={} targetIsAdmin={}",
				requesterId, inputUser.getAuthIdentifier(), inputUser.getIsAdmin());
		if(!requesterCanManageUsers) {
			log.warn("User update denied requester={} targetUserId={}", requesterId, inputUser.getAuthIdentifier());
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update users");
		}

		inputUser.setFirstName(inputUser.getFirstName() == null ? user.getFirstName() : inputUser.getFirstName());
		inputUser.setLastName(inputUser.getLastName() == null ? user.getLastName() : inputUser.getLastName());
		inputUser.setEmail(inputUser.getEmail() == null ? user.getEmail() : inputUser.getEmail());
		inputUser.setAuthIdentifier(inputUser.getAuthIdentifier() == null ? user.getAuthIdentifier() : inputUser.getAuthIdentifier());
		if(inputUser.getIsAdmin() == null) {
			User existingUser = UserGroupManagementAPI.retrieveUserById(inputUser.getAuthIdentifier());
			inputUser.setIsAdmin(existingUser != null && Boolean.TRUE.equals(existingUser.getIsAdmin()));
		}

		EposDataModelDAO.getInstance().clearAllCaches();

		if(UserGroupManagementAPI.createUser(inputUser)) {
			EposDataModelDAO.getInstance().clearAllCaches();
			log.info("User updated targetUserId={} requestedBy={}", inputUser.getAuthIdentifier(), requesterId);

			return new ApiResponseMessage(ApiResponseMessage.OK, "User updated successfully");
		}

		log.error("User update failed targetUserId={} requestedBy={}", inputUser.getAuthIdentifier(), requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update other user");
	}

	public static ApiResponseMessage deleteUser(String instance_id, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();
		String requesterId = user != null ? user.getAuthIdentifier() : null;
		log.debug("User delete request requester={} targetUserId={}", requesterId, instance_id);

		if(user == null || !Boolean.TRUE.equals(user.getIsAdmin())) {
			log.warn("User delete denied requester={} targetUserId={}", requesterId, instance_id);
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete users");
		}

		if(UserGroupManagementAPI.deleteUser(instance_id)){
			EposDataModelDAO.getInstance().clearAllCaches();
			log.info("User deleted targetUserId={} requestedBy={}", instance_id, requesterId);

			return new ApiResponseMessage(ApiResponseMessage.OK, "User deleted successfully");
		}

		log.error("User delete failed targetUserId={} requestedBy={}", instance_id, requesterId);
		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete other user");
	}

	private static boolean isExistingAdmin(User user) {
		if(user == null || user.getAuthIdentifier() == null) return false;

		User persistedUser = UserGroupManagementAPI.retrieveUserById(user.getAuthIdentifier());
		return persistedUser != null && Boolean.TRUE.equals(persistedUser.getIsAdmin());
	}

	private static boolean isBootstrapAdmin(User user) {
		if(user == null || user.getAuthIdentifier() == null) return false;
		if(!Boolean.TRUE.equals(user.getIsAdmin())) return false;

		List<User> allUsers = UserGroupManagementAPI.retrieveAllUsers();
		return allUsers == null || allUsers.isEmpty();
	}

}
