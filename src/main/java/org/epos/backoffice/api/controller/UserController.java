package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import model.RequestStatusType;
import model.RoleType;
import org.epos.backoffice.api.util.AddUserToGroupBean;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.UserManager;
import org.epos.eposdatamodel.User;
import org.epos.eposdatamodel.UserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

@RestController
public class UserController extends ManagementAbstractController<User> implements ApiUserGroupDocTag {

	private static final Logger log = LoggerFactory.getLogger(UserController.class);

	@org.springframework.beans.factory.annotation.Autowired
	public UserController(ObjectMapper objectMapper, HttpServletRequest request) {
		super(objectMapper, request, User.class);
	}

	@RequestMapping(value = "/user",
			produces = {"application/json"},
			method = RequestMethod.PUT)
	@ResponseBody
	@Operation(summary = "Update User", description = "You can use this endpoint to update a User (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly updated.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> put(
			@RequestBody User body
			) {
		User user = getUserFromSession();
		
		System.out.println("Session User: "+user.toString());

		ApiResponseMessage response = UserManager.updateUser(body, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/user",
			produces = {"application/json"},
			method = RequestMethod.POST)
	@ResponseBody
	@Operation(summary = "Create an User", description = "You can use this endpoint to create a User (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly created.", content = @Content(mediaType = "application/json")),
			@ApiResponse(responseCode = "400", description = "Bad request."),
			@ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
			@ApiResponse(responseCode = "404", description = "Not found"),
			@ApiResponse(responseCode = "415", description = "Wrong media type"),
			@ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
	})
	public ResponseEntity<?> post(
			@RequestBody User body
			) {
		User user = getUserFromSession();
		System.out.println("Session User: "+user.toString());

		ApiResponseMessage response = UserManager.createUser(body, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}

	@RequestMapping(value = "/user/{instance_id}",
			produces = {"application/json"},
			method = RequestMethod.GET)
	@ResponseBody
	@Operation(summary = "Get User information", description = "You can use this endpoint to retrieve an User (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "The User is correctly retrieved.", content = @Content(mediaType = "application/json")),
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
		
		ApiResponseMessage response = UserManager.getUser(instance_id, user, available_section);
        System.out.println("Session User: "+response);
		if(response.getCode()==6) return ResponseEntity.status(403).body(response);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response.getListOfUsers());
	}

	@RequestMapping(value = "/user/{instance_id}",
			produces = {"application/json"},
			method = RequestMethod.DELETE)
	@ResponseBody
	@Operation(summary = "Delete a User", description = "You can use this endpoint to delete a User (more information about which fields are required, who has the permission and how to use it are into the BackOffice repository documentation)")
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

		ApiResponseMessage response = UserManager.deleteUser(instance_id, user);
		if(response.getCode()!=4) return ResponseEntity.status(400).body(response);

		return ResponseEntity
				.status(200)
				.body(response);
	}


}
