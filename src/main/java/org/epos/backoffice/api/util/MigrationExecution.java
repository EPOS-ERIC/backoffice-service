package org.epos.backoffice.api.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import metadataapis.DataProductAPI;
import metadataapis.EntityNames;
import metadataapis.SoftwareApplicationAPI;
import metadataapis.SoftwareSourceCodeAPI;
import model.Dataproduct;
import model.StatusType;
import org.epos.eposdatamodel.DataProduct;
import org.epos.eposdatamodel.SoftwareApplication;
import org.epos.eposdatamodel.SoftwareSourceCode;
import org.epos.eposdatamodel.User;

import javax.xml.crypto.Data;
import java.util.Map;


public class MigrationExecution {

    public static JsonArray getListOfObjects(User user) {

        String userId = user != null ? user.getAuthIdentifier() : null;

        if(!user.getIsAdmin()) return null;

        JsonArray list = new JsonArray();

        for(Object object : DataProductAPI.retrieveAPI(EntityNames.DATAPRODUCT.name()).retrieveAllWithStatus(StatusType.PUBLISHED)){
            DataProduct dataProduct = (DataProduct) object;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid",dataProduct.getInstanceId());
            obj.addProperty("metaid",dataProduct.getMetaId());
            obj.addProperty("type","Dataset");
            list.add(obj);
        }

        for(Object object : SoftwareApplicationAPI.retrieveAPI(EntityNames.SOFTWAREAPPLICATION.name()).retrieveAllWithStatus(StatusType.PUBLISHED)){
            SoftwareApplication dataProduct = (SoftwareApplication) object;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid",dataProduct.getInstanceId());
            obj.addProperty("metaid",dataProduct.getMetaId());
            obj.addProperty("type","SoftwareApplication");
            list.add(obj);
        }

        for(Object object : SoftwareSourceCodeAPI.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieveAllWithStatus(StatusType.PUBLISHED)){
            SoftwareSourceCode dataProduct = (SoftwareSourceCode) object;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid",dataProduct.getInstanceId());
            obj.addProperty("metaid",dataProduct.getMetaId());
            obj.addProperty("type","SoftwareSourceCode");
            list.add(obj);
        }

        return list;
    }
}
