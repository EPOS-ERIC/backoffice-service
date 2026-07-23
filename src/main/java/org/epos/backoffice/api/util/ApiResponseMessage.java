package org.epos.backoffice.api.util;

import java.util.List;

import model.MetadataGroup;
import org.epos.eposdatamodel.EPOSDataModelEntity;
import org.epos.eposdatamodel.Group;
import org.epos.eposdatamodel.LinkedEntity;
import org.epos.eposdatamodel.User;

@jakarta.annotation.Generated(value = "io.swagger.codegen.v3.generators.java.SpringCodegen", date = "2022-02-11T09:12:11.707Z[GMT]")
public class ApiResponseMessage {
    public static final int ERROR = 1;
    public static final int WARNING = 2;
    public static final int INFO = 3;
    public static final int OK = 4;
    public static final int TOO_BUSY = 5;
    public static final int UNAUTHORIZED = 6;
    public static final int INTERNAL_ERROR = 7;

    int code;
    String type;
    String message;
    LinkedEntity entity;
    List<? extends EPOSDataModelEntity> listOfEntities;
    List<User> listOfUsers;

    List<Group> listOfGroups;

	public ApiResponseMessage() {
    }

    public ApiResponseMessage(int code, String message) {
        this.code = code;
        switch (code) {
            case ERROR:
                setType("error");
                break;
            case WARNING:
                setType("warning");
                break;
            case INFO:
                setType("info");
                break;
            case OK:
                setType("ok");
                break;
            case TOO_BUSY:
                setType("too busy");
                break;
            case UNAUTHORIZED:
                setType("unauthorized");
                break;
            case INTERNAL_ERROR:
                setType("error");
                break;
            default:
                setType("unknown");
                break;
        }
        this.message = message;
    }

    public ApiResponseMessage(int code, LinkedEntity entity) {
    	this.code = code;
        switch (code) {
            case ERROR:
                setType("error");
                break;
            case WARNING:
                setType("warning");
                break;
            case INFO:
                setType("info");
                break;
            case OK:
                setType("ok");
                break;
            case TOO_BUSY:
                setType("too busy");
                break;
            case UNAUTHORIZED:
                setType("unauthorized");
                break;
            case INTERNAL_ERROR:
                setType("error");
                break;
            default:
                setType("unknown");
                break;
        }
        this.entity = entity;
	}

	public ApiResponseMessage(int code, List<? extends EPOSDataModelEntity> list) {
		 this.code = code;
	        switch (code) {
	            case ERROR:
	                setType("error");
	                break;
	            case WARNING:
	                setType("warning");
	                break;
	            case INFO:
	                setType("info");
	                break;
	            case OK:
	                setType("ok");
	                break;
	            case TOO_BUSY:
	                setType("too busy");
	                break;
	            case UNAUTHORIZED:
	                setType("unauthorized");
	                break;
	            case INTERNAL_ERROR:
	                setType("error");
	                break;
	            default:
	                setType("unknown");
	                break;
	        }
	        this.listOfEntities = list;
	}
	
	public ApiResponseMessage(int code, boolean userManagement, List<User> list) {
		 this.code = code;
	        switch (code) {
	            case ERROR:
	                setType("error");
	                break;
	            case WARNING:
	                setType("warning");
	                break;
	            case INFO:
	                setType("info");
	                break;
	            case OK:
	                setType("ok");
	                break;
	            case TOO_BUSY:
	                setType("too busy");
	                break;
	            case UNAUTHORIZED:
	                setType("unauthorized");
	                break;
	            case INTERNAL_ERROR:
	                setType("error");
	                break;
	            default:
	                setType("unknown");
	                break;
	        }
	        this.listOfUsers = list;
	}

    public ApiResponseMessage(int code, boolean userManagement, boolean groupManagement, List<Group> list) {
        this.code = code;
        switch (code) {
            case ERROR:
                setType("error");
                break;
            case WARNING:
                setType("warning");
                break;
            case INFO:
                setType("info");
                break;
            case OK:
                setType("ok");
                break;
            case TOO_BUSY:
                setType("too busy");
                break;
            case UNAUTHORIZED:
                setType("unauthorized");
                break;
            case INTERNAL_ERROR:
                setType("error");
                break;
            default:
                setType("unknown");
                break;
        }
        this.listOfGroups = list;
    }


	public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
    
    public LinkedEntity getEntity() {
        return entity;
    }

    public void setEntity(LinkedEntity entity) {
        this.entity = entity;
    }
    
    public List<? extends EPOSDataModelEntity> getListOfEntities() {
        return listOfEntities;
    }

    public void setEntity(List<? extends EPOSDataModelEntity> listOfEntities) {
        this.listOfEntities = listOfEntities;
    }
    

    public List<User> getListOfUsers() {
		return listOfUsers;
	}

	public void setListOfUsers(List<User> listOfUsers) {
		this.listOfUsers = listOfUsers;
	}

    public List<Group> getListOfGroups() {
        return listOfGroups;
    }

    public void setListOfGroups(List<Group> listOfUsers) {
        this.listOfGroups = listOfGroups;
    }

	@Override
	public String toString() {
		return "ApiResponseMessage [code=" + code + ", type=" + type + ", message=" + message + ", entity=" + entity
				+ ", listOfEntities=" + listOfEntities + ", listOfUsers=" + listOfUsers + "]";
	}


}
