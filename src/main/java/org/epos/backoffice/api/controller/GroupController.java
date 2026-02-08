package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.epos.backoffice.api.util.*;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;

@RestController
public class GroupController extends ManagementAbstractController<Group> implements ApiUserGroupDocTag {

	private static final Logger log = LoggerFactory.getLogger(GroupController.class);

	@org.springframework.beans.factory.annotation.Autowired
	public GroupController(ObjectMapper objectMapper, HttpServletRequest request) {
		super(objectMapper, request, Group.class);
	}

	@RequestMapping(value = "/group",
			produces = {"application/json"},
			method = RequestMethod.PUT)
	@ResponseBody
	@Operation(summary = "Update Group", description = "You can use this endpoint to update a Group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The Group is correctly updated.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> put(
			@RequestBody Group body
			) {
		User user = getUserFromSession();
		
		System.out.println("Session User: "+user.toString());

		
		ApiResponseMessage response = GroupManager.updateGroup(body, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Create an Group", description = "You can use this endpoint to create a Group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The Group is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> post(
			@RequestBody Group body
			) {

		User user = getUserFromSession();

		System.out.println("Session User: "+user.toString());

		ApiResponseMessage response = GroupManager.createGroup(body, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group/{instance_id}",
			produces = {"application/json"},
			method = RequestMethod.GET)
	@ResponseBody
	@Operation(summary = "Get User information", description = "You can use this endpoint to retrieve an Group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The Group is correctly retrieved.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> get(
			@PathVariable String instance_id,
			@RequestParam(required = false, defaultValue = "false") Boolean available_section
			) {
		if (instance_id == null)
			return ResponseEntity
					.status(400)
					.body(new ApiResponseMessage(1, "The [instance_id] field can't be left blank"));

		User user = getUserFromSession();

		System.out.println("Session User: "+user.toString());
		
		ApiResponseMessage response = GroupManager.getGroup(instance_id, user, available_section);
		if(response.getCode()==6) return ResponseEntity.status(403).body(response);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response.getListOfGroups());
	}

	@RequestMapping(value = "/group/{instance_id}",
			produces = {"application/json"},
			method = RequestMethod.DELETE)
	@ResponseBody
	@Operation(summary = "Delete a Group", description = "You can use this endpoint to delete a Group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly deleted.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> delete(
			@PathVariable String instance_id
			) {
		User user = getUserFromSession();

		System.out.println("Session User: "+user.toString());

		ApiResponseMessage response = GroupManager.deleteGroup(instance_id, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group/addUserToGroup",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Add User to Group with permissions", description = "You can use this endpoint to add a User in a group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> post(
			@RequestBody AddUserToGroupBean addUserToGroupBean
	) {
		User user = getUserFromSession();
		System.out.println("Session User: "+ user.toString());

		System.out.println(addUserToGroupBean.toString());

		ApiResponseMessage response = UserManager.addUserToGroup(addUserToGroupBean, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group/updateUserInGroup",
			produces = {"application/json"},
			method = RequestMethod.PUT)
	@ResponseBody
	@Operation(summary = "Update User in Group with permissions", description = "You can use this endpoint to update a User in a group (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> put(
			@RequestBody AddUserToGroupBean addUserToGroupBean
	) {
		User user = getUserFromSession();
		System.out.println("Session User: "+ user.toString());

		ApiResponseMessage response = UserManager.updateUserToGroup(addUserToGroupBean, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group/removeUserFromGroup",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Remove User from Group with permissions", description = "You can use this endpoint to remove a User from a group(more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> delete(
			@RequestBody RemoveUserFromGroupBean removeUserFromGroupBean
	) {
		User user = getUserFromSession();
		System.out.println("Session User: "+ user.toString());
		System.out.println("DEBUG: "+removeUserFromGroupBean);

		ApiResponseMessage response = UserManager.removeUserFromGroup(removeUserFromGroupBean, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}


	@RequestMapping(value = "/group/addEntityToGroup",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Add Entity to Group with permissions", description = "You can use this endpoint to add an Entity to a Group")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The Entity is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> post(
			@RequestBody AddEntityToGroupBean addEntityToGroupBean
	) {
		User user = getUserFromSession();
		System.out.println("Session User: "+ user.toString());

		System.out.println(addEntityToGroupBean.toString());

		ApiResponseMessage response = GroupManager.addEntityToGroup(addEntityToGroupBean, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/group/removeEntityFromGroup",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Remove Entity from Group with permissions", description = "You can use this endpoint to remove an Entity from a Group")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The Entity is correctly removed.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> delete(
			@RequestBody AddEntityToGroupBean addEntityToGroupBean
	) {
		User user = getUserFromSession();
		System.out.println("Session User: "+ user.toString());

		ApiResponseMessage response = GroupManager.removeEntityFromGroup(addEntityToGroupBean, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}


}
