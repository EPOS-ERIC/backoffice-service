package org.epos.backoffice.api.controller;

import abstractapis.AbstractAPI;
import com.fasterxml.jackson.databind.ObjectMapper;
import metadataapis.EntityNames;
import org.epos.backoffice.api.util.ApiResponseMessage;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.User;
import org.springframework.http.ResponseEntity;

import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public abstract class ManagementAbstractController<T> extends BasicController<T> {

	protected static AbstractAPI dbapi;

	public ManagementAbstractController(ObjectMapper objectMapper, HttpServletRequest request, Class<T> entityType) {
		super(objectMapper, request, entityType);
	}
	protected ResponseEntity<?> getMethod(String meta_id, String instance_id, Boolean available_section) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name());
		if (meta_id == null)
			return ResponseEntity
					.status(400)
					.body(new ApiResponseMessage(1, "The [meta_id] field can't be left blank"));
		if(instance_id == null) {
			instance_id = "all";
		}

		User user = getUserFromSession();

		List<T> revertedList = new ArrayList<>();
		List<T> list = new ArrayList<T>();

		if(!entityType.equals(User.class)) {
			List items = (List) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(meta_id,instance_id,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType).getListOfEntities();
			if(items!=null) {
				list.addAll(items);
			}
		}

		list.forEach(e -> revertedList.add(0, e));

		if (list.isEmpty())
			return ResponseEntity.status(404).body("[]");

		return ResponseEntity
				.status(200)
				.body(gson.toJson(revertedList));
	}


	protected ResponseEntity<?> deleteMethod(String instance_id) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name());
		if (instance_id == null)
			return ResponseEntity
					.status(400)
					.body(new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank"));

		User user = getUserFromSession();
		if (user == null) {
			return ResponseEntity.status(401).body("Unauthorized: User not found in session.");
		}

		if(!entityType.equals(User.class)) {
			ApiResponseMessage response = EPOSDataModelManager.deleteEposDataModelEntity(instance_id,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
			if(response.getCode() == ApiResponseMessage.OK) {
				return ResponseEntity.status(200).body(response);
			} else if(response.getCode() == ApiResponseMessage.UNAUTHORIZED) {
				return ResponseEntity.status(403).body(response);
			} else {
				return ResponseEntity.status(400).body(response);
			}
		}

		return ResponseEntity.status(400).body(new ApiResponseMessage(ApiResponseMessage.ERROR, "Instance not deleted"));
	}

	protected ResponseEntity<?> postMethod(EPOSDataModelEntity body, boolean takeCareOfTheParent) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name());

		User user = getUserFromSession();
		if (user == null) {
			return ResponseEntity.status(401).body("Unauthorized: User not found in session.");
		}

		if(!entityType.equals(User.class)) {
			ApiResponseMessage response = EPOSDataModelManager.createEposDataModelEntity(body,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
			if(response.getCode() == ApiResponseMessage.OK) {
				return ResponseEntity.status(201).body(response.getEntity());
			} else if(response.getCode() == ApiResponseMessage.UNAUTHORIZED) {
				return ResponseEntity.status(403).body(response.getMessage());
			} else {
				return ResponseEntity.status(400).body(response.getMessage());
			}
		}

		return ResponseEntity.status(400).body(null);
	}

	protected ResponseEntity<?> updateMethod(EPOSDataModelEntity body, boolean takeCareOfTheParent) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name());

		User user = getUserFromSession();
		if (user == null) {
			return ResponseEntity.status(401).body("Unauthorized: User not found in session.");
		}

		if(!entityType.equals(User.class)) {
			ApiResponseMessage response = EPOSDataModelManager.updateEposDataModelEntity(body,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
			if(response.getCode() == ApiResponseMessage.OK) {
				return ResponseEntity.status(201).body(response.getEntity());
			} else if(response.getCode() == ApiResponseMessage.UNAUTHORIZED) {
				return ResponseEntity.status(403).body(response.getMessage());
			} else {
				return ResponseEntity.status(400).body(response.getMessage());
			}
		}

		return ResponseEntity.status(400).body(null);
	}

}