package org.epos.backoffice.api.controller;

import abstractapis.AbstractAPI;
import com.fasterxml.jackson.databind.ObjectMapper;
import metadataapis.EntityNames;
import org.epos.backoffice.api.exception.ApiResponseMessage;
import org.epos.backoffice.api.util.EPOSDataModelManager;
import org.epos.eposdatamodel.*;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletRequest;


import java.util.*;

public abstract class MetadataAbstractController<T extends EPOSDataModelEntity> extends AbstractController<T> {

	protected static AbstractAPI dbapi;

	public MetadataAbstractController(ObjectMapper objectMapper, HttpServletRequest request, Class<T> entityType) {
		super(objectMapper, request, entityType);
	}
	protected ResponseEntity<?> getMethod(String meta_id, String instance_id, Boolean available_section) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name(),entityType);
		if (meta_id == null)
			return ResponseEntity
					.status(400)
					.body(new ApiResponseMessage(1, "The [meta_id] field can't be left blank"));
		if(instance_id == null) {
			instance_id = "all";
		}

		User user = getUserFromSession();
		
		System.out.println(user);

		List<T> revertedList = new ArrayList<>();
		List<T> list = new ArrayList<T>();

		if(!entityType.equals(User.class)) {
			list.addAll(
					(Collection<? extends T>) EPOSDataModelManager.getEPOSDataModelEposDataModelEntity(meta_id,instance_id,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType).getListOfEntities());
		}

		list.forEach(e -> revertedList.add(0, e));

		if (list.isEmpty())
			return ResponseEntity.status(404).body("[]");

		return ResponseEntity
				.status(200)
				.body(gson.toJson(revertedList));
	}


	protected ResponseEntity<?> deleteMethod(String instance_id) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name(),entityType);
		if (instance_id == null)
			return ResponseEntity
					.status(400)
					.body(new ApiResponseMessage(ApiResponseMessage.ERROR, "The [instance_id] field can't be left blank"));

		User user = getUserFromSession();

		boolean done = false;

		if(!entityType.equals(User.class)) {
			done = EPOSDataModelManager.deleteEposDataModelEntity(instance_id,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
		}

		if(done) return ResponseEntity.status(200).body(new ApiResponseMessage(ApiResponseMessage.OK, "Instance deleted"));
		else return ResponseEntity.status(400).body(new ApiResponseMessage(ApiResponseMessage.ERROR, "Instance not deleted"));
	}

	protected ResponseEntity<?> postMethod(EPOSDataModelEntity body, boolean takeCareOfTheParent) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name(),entityType);

		User user = getUserFromSession();
		if(!entityType.equals(User.class)) {
			ApiResponseMessage response = EPOSDataModelManager.createEposDataModelEntity(body,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
			if(response.getCode()==ApiResponseMessage.OK) return ResponseEntity.status(201).body(response.getEntity());
			else return ResponseEntity.status(400).body(response.getMessage());
		}


		return ResponseEntity.status(400).body(null);
	}

	protected ResponseEntity<?> updateMethod(EPOSDataModelEntity body, boolean takeCareOfTheParent) {
		dbapi = AbstractAPI.retrieveAPI(EntityNames.valueOf(entityType.getSimpleName().toUpperCase()).name(),entityType);

		User user = getUserFromSession();
		if(!entityType.equals(User.class)) {
			ApiResponseMessage response = EPOSDataModelManager.updateEposDataModelEntity(body,user,EntityNames.valueOf(entityType.getSimpleName().toUpperCase()),entityType);
			if(response.getCode()==ApiResponseMessage.OK) return ResponseEntity.status(201).body(response.getEntity());
			else return ResponseEntity.status(400).body(response.getMessage());
		}


		return ResponseEntity.status(400).body(null);
	}

}