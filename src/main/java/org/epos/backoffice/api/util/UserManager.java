package org.epos.backoffice.api.util;


import java.util.*;
import java.util.stream.Collectors;

import dao.EposDataModelDAO;
import model.RequestStatusType;
import model.RoleType;
import model.StatusType;
import org.epos.eposdatamodel.User;
import usermanagementapis.UserGroupManagementAPI;

public class UserManager {


	public static ApiResponseMessage getUser(String instance_id, User user, Boolean available_section) {

		if (instance_id == null)
			return new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank");

		List<User> personList;
		if(instance_id.equals("self")){
			User tempUser = UserGroupManagementAPI.retrieveUserById(user.getAuthIdentifier());
			personList = tempUser!=null? List.of(tempUser) : new ArrayList<>();
            System.out.println("USER STREAM SELF: "+personList);
		}
		else if(instance_id.equals("all")){
			personList = UserGroupManagementAPI.retrieveAllUsers();
            System.out.println("USER STREAM ALL: "+personList);
		} else {
			User tempUser = UserGroupManagementAPI.retrieveUserById(instance_id);
			personList = tempUser!=null? List.of(tempUser) : new ArrayList<>();
            System.out.println("ELSE STREAM ALL: "+personList);
		}

		List<User> userStream = personList.stream()
				.filter(x -> x.getAuthIdentifier() != null && !x.getAuthIdentifier().isEmpty()).collect(Collectors.toList());

        System.out.println("USER STREAM: "+userStream);

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

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't register other user");

		inputUser.setFirstName(inputUser.getFirstName() == null ? user.getFirstName() : inputUser.getFirstName());
		inputUser.setLastName(inputUser.getLastName() == null ? user.getLastName() : inputUser.getLastName());
		inputUser.setEmail(inputUser.getEmail() == null ? user.getEmail() : inputUser.getEmail());
		inputUser.setAuthIdentifier(inputUser.getAuthIdentifier() == null ? user.getAuthIdentifier() : inputUser.getAuthIdentifier());
		inputUser.setIsAdmin(inputUser.getIsAdmin() == null ? user.getIsAdmin() : inputUser.getIsAdmin());

		if (UserGroupManagementAPI.createUser(inputUser)) {
			return new ApiResponseMessage(ApiResponseMessage.OK, "User created successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't register other user");
	}

	public static ApiResponseMessage addUserToGroup(AddUserToGroupBean userGroup, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();

		if(!user.getIsAdmin() && userGroup.getStatusType().equals("ACCEPTED")) return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't add users to groups");

		Boolean result = UserGroupManagementAPI.addUserToGroup(
				userGroup.getGroupid(),
				userGroup.getUserid(),
				RoleType.valueOf(userGroup.getRole()),
				RequestStatusType.valueOf(userGroup.getStatusType()));

		if(result!=null && result)
			return new ApiResponseMessage(ApiResponseMessage.OK, "User added successfully");

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't add the user to group");
	}


	public static ApiResponseMessage removeUserFromGroup(RemoveUserFromGroupBean removeUserFromGroupBean, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't remove users from groups");

		Boolean result = UserGroupManagementAPI.removeUserFromGroup(
				removeUserFromGroupBean.getGroupid(),
				removeUserFromGroupBean.getUserid());

		if(result!=null && result)
			return new ApiResponseMessage(ApiResponseMessage.OK, "User removed successfully from group");

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't remove the user from group");

	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static ApiResponseMessage updateUser(User inputUser, User user) {

		EposDataModelDAO.getInstance().clearAllCaches();
		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update users");

		inputUser.setFirstName(inputUser.getFirstName() == null ? user.getFirstName() : inputUser.getFirstName());
		inputUser.setLastName(inputUser.getLastName() == null ? user.getLastName() : inputUser.getLastName());
		inputUser.setEmail(inputUser.getEmail() == null ? user.getEmail() : inputUser.getEmail());
		inputUser.setAuthIdentifier(inputUser.getAuthIdentifier() == null ? user.getAuthIdentifier() : inputUser.getAuthIdentifier());
		inputUser.setIsAdmin(inputUser.getIsAdmin() == null ? user.getIsAdmin() : inputUser.getIsAdmin());

		if(UserGroupManagementAPI.createUser(inputUser)) {

			return new ApiResponseMessage(ApiResponseMessage.OK, "User updated successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't update other user");
	}

	public static ApiResponseMessage deleteUser(String instance_id, User user) {
		EposDataModelDAO.getInstance().clearAllCaches();

		if(!user.getIsAdmin()) return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete users");

		if(UserGroupManagementAPI.deleteUser(instance_id)){

			return new ApiResponseMessage(ApiResponseMessage.OK, "User deleted successfully");
		}

		return new ApiResponseMessage(ApiResponseMessage.ERROR, "You can't delete other user");
	}

}
