package org.epos.backoffice.api.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import commonapis.LinkedEntityAPI;
import metadataapis.*;
import model.StatusType;
import org.epos.backoffice.bean.*;
import org.epos.eposdatamodel.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class MigrationExecution {

    public static JsonArray getListOfObjects(User user) {

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
            SoftwareApplication softwareApplication = (SoftwareApplication) object;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid",softwareApplication.getInstanceId());
            obj.addProperty("metaid",softwareApplication.getMetaId());
            obj.addProperty("type","SoftwareApplication");
            list.add(obj);
        }

        for(Object object : SoftwareSourceCodeAPI.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieveAllWithStatus(StatusType.PUBLISHED)){
            SoftwareSourceCode softwareSourceCode = (SoftwareSourceCode) object;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid",softwareSourceCode.getInstanceId());
            obj.addProperty("metaid",softwareSourceCode.getMetaId());
            obj.addProperty("type","SoftwareSourceCode");
            list.add(obj);
        }

        return list;
    }

    public static JsonObject getDataset(User user) {

        if(!user.getIsAdmin()) return null;

        JsonObject item = new JsonObject();

        String type = null;

        DataProduct dataProduct = (DataProduct) DataProductAPI.retrieveAPI(EntityNames.DATAPRODUCT.name()).retrieveAllWithStatus(StatusType.PUBLISHED).get(0);
        if(dataProduct.getKeywords().contains("pointclick")) type = "DYNAMIC";
        if(type == null) {
            for (Object obj : dataProduct.getDistribution()){
                Distribution distribution = (Distribution) DistributionAPI.retrieveAPI(EntityNames.DISTRIBUTION.name()).retrieve(((Distribution)obj).getInstanceId());
                for(Object objInner : distribution.getSupportedOperation()) {
                    Operation operation = (Operation) OperationAPI.retrieveAPI(EntityNames.OPERATION.name()).retrieve(((Operation)objInner).getInstanceId());
                    if (operation.getTemplate().contains("WMS")) type = "WMS";
                    if (operation.getTemplate().contains("WFS")) type = "WFS";
                    if (operation.getTemplate().contains("WMTS")) type = "WMTS";
                }
            }
            if(type == null) type = "HTTP";
        }

        if(type.equals("DYNAMIC")){
            DynamicDatasetExport dynamicDatasetExport = new DynamicDatasetExport();
            dynamicDatasetExport.setResourceType("dataset");

            DynamicDatasetExport.Metadata metadata = new DynamicDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(dataProduct.getCategory().stream().map(cat -> CategoryAPI.retrieveAPI(EntityNames.CATEGORY.name()).retrieve(cat.getInstanceId())).map(catName -> ((Category)catName).getName()).collect(Collectors.toList()));
            metadata.setContactPoints(
                    dataProduct.getContactPoint().stream()
                            .map(cat -> ContactPointAPI.retrieveAPI(EntityNames.CONTACTPOINT.name()).retrieve(cat.getInstanceId()))
                            .flatMap(catName -> ((ContactPoint) catName).getEmail().stream())
                            .collect(Collectors.toList())
            );
            metadata.setCreated(dataProduct.getCreated().toString());
            metadata.setDescription(dataProduct.getDescription().getFirst());
            metadata.setKeywords(Arrays.stream(dataProduct.getKeywords().split(",")).toList());
            List<DynamicDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for(LinkedEntity identifier : dataProduct.getIdentifier()) {
                Identifier ide = (Identifier) LinkedEntityAPI.retrieveFromLinkedEntity(identifier);
                DynamicDatasetExport.PersistentIdentifier pid = new DynamicDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(dataProduct.getPublisher().stream()
                    .map(cat -> OrganizationAPI.retrieveAPI(EntityNames.ORGANIZATION.name()).retrieve(cat.getInstanceId()))
                    .flatMap(catName -> ((Organization) catName).getLegalName().stream())
                    .collect(Collectors.toList()));
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            DynamicDatasetExport.TemporalCoverage gtc = new DynamicDatasetExport.TemporalCoverage();
            for(LinkedEntity tpe : dataProduct.getTemporalExtent()) {
                PeriodOfTime pot = (PeriodOfTime) LinkedEntityAPI.retrieveFromLinkedEntity(tpe);
                gtc.setStart(pot.getStartDate().toString());
                gtc.setEnd(pot.getEndDate().toString());
                metadata.setTemporalCoverage(gtc);
            }
            for(LinkedEntity tpe : dataProduct.getSpatialExtent()) {
                Location pot = (Location) LinkedEntityAPI.retrieveFromLinkedEntity(tpe);
                metadata.setSpatialCoverage(pot.getLocation());
            }
            metadata.setTitle(dataProduct.getTitle().get(0));
            metadata.setUpdated(dataProduct.getModified().toString());

            List<DynamicDatasetExport.Distribution> distributions = new ArrayList<>();
            for(LinkedEntity dist : dataProduct.getDistribution()){
                Distribution distribution = (Distribution) LinkedEntityAPI.retrieveFromLinkedEntity(dist);
                DynamicDatasetExport.Distribution dd = new DynamicDatasetExport.Distribution();
                dd.setByteSize(Long.valueOf(distribution.getByteSize()));
                dd.setDescription(distribution.getDescription().get(0));
                DynamicDatasetExport.DownloadableFile downloadableFile = new DynamicDatasetExport.DownloadableFile();
                downloadableFile.setFormat(distribution.getFormat());
                downloadableFile.setUrl(distribution.getDownloadURL().get(0));
                dd.setDownloadableFile(downloadableFile);
                dd.setLicense(distribution.getLicence());
                dd.setTitle(distribution.getTitle().get(0));
                dd.setType(distribution.getType());

                WebService webService = (WebService) LinkedEntityAPI.retrieveFromLinkedEntity(distribution.getAccessService().get(0));

                DynamicDatasetExport.WebService webService1 = new DynamicDatasetExport.WebService();
                webService1.setContactPoints(webService.getContactPoint().stream()
                        .map(cat -> ContactPointAPI.retrieveAPI(EntityNames.CONTACTPOINT.name()).retrieve(cat.getInstanceId()))
                        .flatMap(catName -> ((ContactPoint) catName).getEmail().stream())
                        .collect(Collectors.toList()));
                webService1.setFormat(distribution.getFormat());
                webService1.setProvider(((Organization)LinkedEntityAPI.retrieveFromLinkedEntity(webService.getProvider())).getLegalName().get(0));
                webService1.setType("DYNAMIC");
                webService1.setDocumentationUri(((Documentation)LinkedEntityAPI.retrieveFromLinkedEntity(webService.getDocumentation().get(0))).getUri());

                Operation operation = (Operation) LinkedEntityAPI.retrieveFromLinkedEntity(distribution.getSupportedOperation().get(0));
                webService1.setMethod(operation.getMethod());
                webService1.setTemplateUrl(operation.getTemplate());

                List<DynamicDatasetExport.Parameter> parameters = new ArrayList<>();
                int position = 0;
                for(LinkedEntity mapping : operation.getMapping()){
                    Mapping mapping1 = (Mapping) LinkedEntityAPI.retrieveFromLinkedEntity(mapping);
                    DynamicDatasetExport.Parameter parameter = new DynamicDatasetExport.Parameter();
                    parameter.setMultipleValues(Boolean.valueOf(mapping1.getMultipleValues()));
                    parameter.setDefaultValues(Collections.singletonList(mapping1.getDefaultValue()));
                    parameter.setEnumValues(mapping1.getParamValue());
                    parameter.setLabel(mapping1.getLabel());
                    parameter.setName(mapping1.getVariable());
                    parameter.setPattern(mapping1.getValuePattern());
                    parameter.setPosition(position);
                    parameter.setProperty(mapping1.getProperty());
                    parameter.setRequired(Boolean.valueOf(mapping1.getRequired()));
                    parameter.setReadOnly(Boolean.valueOf(mapping1.getReadOnlyValue()));
                    for(String val : mapping1.getParamValue()){
                        if(val.contains("covjson") || val.contains("cov+json")) webService1.setVisibleOn(List.of("GRAPH", "MAP"));
                        if(val.contains("geojson") || val.contains("geo+json")) webService1.setVisibleOn(List.of("TABLE", "MAP"));
                    }
                    parameters.add(parameter);
                    position++;
                }

                webService1.setParameters(parameters);

                dd.setWebService(webService1);
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            dynamicDatasetExport.setMetadata(metadata);
        }

        if(type.equals("HTTP")){
            HTTPDatasetExport httpDatasetExport = new HTTPDatasetExport();
            httpDatasetExport.setResourceType("dataset");

            HTTPDatasetExport.Metadata metadata = new HTTPDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(dataProduct.getCategory().stream().map(cat -> CategoryAPI.retrieveAPI(EntityNames.CATEGORY.name()).retrieve(cat.getInstanceId())).map(catName -> ((Category)catName).getName()).collect(Collectors.toList()));
            metadata.setContactPoints(
                    dataProduct.getContactPoint().stream()
                            .map(cat -> ContactPointAPI.retrieveAPI(EntityNames.CONTACTPOINT.name()).retrieve(cat.getInstanceId()))
                            .flatMap(catName -> ((ContactPoint) catName).getEmail().stream())
                            .collect(Collectors.toList())
            );
            metadata.setCreated(dataProduct.getCreated().toString());
            metadata.setDescription(dataProduct.getDescription().getFirst());
            metadata.setKeywords(Arrays.stream(dataProduct.getKeywords().split(",")).toList());
            List<HTTPDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for(LinkedEntity identifier : dataProduct.getIdentifier()) {
                Identifier ide = (Identifier) LinkedEntityAPI.retrieveFromLinkedEntity(identifier);
                HTTPDatasetExport.PersistentIdentifier pid = new HTTPDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(dataProduct.getPublisher().stream()
                    .map(cat -> OrganizationAPI.retrieveAPI(EntityNames.ORGANIZATION.name()).retrieve(cat.getInstanceId()))
                    .flatMap(catName -> ((Organization) catName).getLegalName().stream())
                    .collect(Collectors.toList()));
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            HTTPDatasetExport.TemporalCoverage gtc = new HTTPDatasetExport.TemporalCoverage();
            for(LinkedEntity tpe : dataProduct.getTemporalExtent()) {
                PeriodOfTime pot = (PeriodOfTime) LinkedEntityAPI.retrieveFromLinkedEntity(tpe);
                gtc.setStart(pot.getStartDate().toString());
                gtc.setEnd(pot.getEndDate().toString());
                metadata.setTemporalCoverage(gtc);
            }
            for(LinkedEntity tpe : dataProduct.getSpatialExtent()) {
                Location pot = (Location) LinkedEntityAPI.retrieveFromLinkedEntity(tpe);
                metadata.setSpatialCoverage(pot.getLocation());
            }
            metadata.setTitle(dataProduct.getTitle().get(0));
            metadata.setUpdated(dataProduct.getModified().toString());

            List<HTTPDatasetExport.Distribution> distributions = new ArrayList<>();
            for(LinkedEntity dist : dataProduct.getDistribution()){
                Distribution distribution = (Distribution) LinkedEntityAPI.retrieveFromLinkedEntity(dist);
                HTTPDatasetExport.Distribution dd = new HTTPDatasetExport.Distribution();
                dd.setByteSize(Long.valueOf(distribution.getByteSize()));
                dd.setDescription(distribution.getDescription().get(0));
                HTTPDatasetExport.DownloadableFile downloadableFile = new HTTPDatasetExport.DownloadableFile();
                downloadableFile.setFormat(distribution.getFormat());
                downloadableFile.setUrl(distribution.getDownloadURL().get(0));
                dd.setDownloadableFile(downloadableFile);
                dd.setLicense(distribution.getLicence());
                dd.setTitle(distribution.getTitle().get(0));
                dd.setType(distribution.getType());

                WebService webService = (WebService) LinkedEntityAPI.retrieveFromLinkedEntity(distribution.getAccessService().get(0));

                HTTPDatasetExport.WebService webService1 = new HTTPDatasetExport.WebService();
                webService1.setContactPoints(webService.getContactPoint().stream()
                        .map(cat -> ContactPointAPI.retrieveAPI(EntityNames.CONTACTPOINT.name()).retrieve(cat.getInstanceId()))
                        .flatMap(catName -> ((ContactPoint) catName).getEmail().stream())
                        .collect(Collectors.toList()));
                webService1.setFormat(distribution.getFormat());
                webService1.setProvider(((Organization)LinkedEntityAPI.retrieveFromLinkedEntity(webService.getProvider())).getLegalName().get(0));
                webService1.setType("DYNAMIC");
                webService1.setDocumentationUri(((Documentation)LinkedEntityAPI.retrieveFromLinkedEntity(webService.getDocumentation().get(0))).getUri());

                Operation operation = (Operation) LinkedEntityAPI.retrieveFromLinkedEntity(distribution.getSupportedOperation().get(0));
                webService1.setMethod(operation.getMethod());
                webService1.setTemplateUrl(operation.getTemplate());

                List<HTTPDatasetExport.Parameter> parameters = new ArrayList<>();
                int position = 0;
                for(LinkedEntity mapping : operation.getMapping()){
                    Mapping mapping1 = (Mapping) LinkedEntityAPI.retrieveFromLinkedEntity(mapping);
                    HTTPDatasetExport.Parameter parameter = new HTTPDatasetExport.Parameter();
                    parameter.setMultipleValues(Boolean.valueOf(mapping1.getMultipleValues()));
                    parameter.setDefaultValues(Collections.singletonList(mapping1.getDefaultValue()));
                    parameter.setEnumValues(mapping1.getParamValue());
                    parameter.setLabel(mapping1.getLabel());
                    parameter.setName(mapping1.getVariable());
                    parameter.setPattern(mapping1.getValuePattern());
                    parameter.setPosition(position);
                    parameter.setProperty(mapping1.getProperty());
                    parameter.setRequired(Boolean.valueOf(mapping1.getRequired()));
                    parameter.setReadOnly(Boolean.valueOf(mapping1.getReadOnlyValue()));
                    for(String val : mapping1.getParamValue()){
                        if(val.contains("covjson") || val.contains("cov+json")) webService1.setVisibleOn(List.of("GRAPH", "MAP"));
                        if(val.contains("geojson") || val.contains("geo+json")) webService1.setVisibleOn(List.of("TABLE", "MAP"));
                    }
                    parameters.add(parameter);
                    position++;
                }

                webService1.setParameters(parameters);

                dd.setWebService(webService1);
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            httpDatasetExport.setMetadata(metadata);
        }

        if(type.equals("WMS")) {
            WMSDatasetExport wmsDatasetExport = new WMSDatasetExport();
            wmsDatasetExport.setResourceType("dataset");

            WMSDatasetExport.Metadata metadata = new WMSDatasetExport.Metadata();



            wmsDatasetExport.setMetadata(metadata);
        }

        if(type.equals("WMTS")){
            WMTSDatasetExport wmtsDatasetExport = new WMTSDatasetExport();
            wmtsDatasetExport.setResourceType("dataset");

            WMTSDatasetExport.Metadata metadata = new WMTSDatasetExport.Metadata();



            wmtsDatasetExport.setMetadata(metadata);
        }

        if(type.equals("WFS")){
            WFSDatasetExport wfsDatasetExport = new WFSDatasetExport();
            wfsDatasetExport.setResourceType("dataset");

            WFSDatasetExport.Metadata metadata = new WFSDatasetExport.Metadata();



            wfsDatasetExport.setMetadata(metadata);
        }

        return item;
    }

    public static JsonObject getSoftwareApplication(User user) {

        if(!user.getIsAdmin()) return null;

        JsonObject item = new JsonObject();

        SoftwareApplication softwareApplication = (SoftwareApplication) SoftwareApplicationAPI.retrieveAPI(EntityNames.SOFTWAREAPPLICATION.name()).retrieveAllWithStatus(StatusType.PUBLISHED).get(0);

        return item;
    }

    public static JsonObject getSoftwareSourceCode(User user) {

        if(!user.getIsAdmin()) return null;

        JsonObject item = new JsonObject();

        SoftwareSourceCode softwareSourceCode = (SoftwareSourceCode) SoftwareSourceCodeAPI.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieveAllWithStatus(StatusType.PUBLISHED).get(0);

        return item;
    }
}
