package org.epos.backoffice.api.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import commonapis.LinkedEntityAPI;
import metadataapis.*;
import model.StatusType;
import org.epos.backoffice.bean.*;
import org.epos.eposdatamodel.*;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class MigrationExecution {

    public static Gson gson = new Gson();

    // =====================================================================
    // Null / existence check helpers
    // =====================================================================

    private static boolean isAdmin(User user) {
        return user != null && Boolean.TRUE.equals(user.getIsAdmin());
    }

    /** Returns an empty list instead of null. */
    private static <T> List<T> safe(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    /** First non-null element of the list, or null if the list is null/empty. */
    private static <T> T first(List<T> list) {
        if (list == null) return null;
        for (T t : list) {
            if (t != null) return t;
        }
        return null;
    }

    private static String str(Object o) {
        return o == null ? null : o.toString();
    }

    private static boolean bool(Object o) {
        return o != null && Boolean.parseBoolean(o.toString());
    }

    private static <T> List<T> singletonOrEmpty(T value) {
        List<T> out = new ArrayList<>();
        if (value != null) out.add(value);
        return out;
    }

    /** Safe cast: null if the object is null or not of the expected type. */
    private static <T> T cast(Object o, Class<T> cls) {
        return cls.isInstance(o) ? cls.cast(o) : null;
    }

    /** Resolves a LinkedEntity, checking that it exists and is of the expected type. */
    private static <T> T resolve(LinkedEntity le, Class<T> cls) {
        if (le == null) return null;
        return cast(LinkedEntityAPI.retrieveFromLinkedEntity(le), cls);
    }

    private static Long parseLong(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Long.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static List<String> splitKeywords(String keywords) {
        if (keywords == null || keywords.isBlank()) return new ArrayList<>();
        return Arrays.stream(keywords.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static List<String> categoryNames(List<LinkedEntity> refs) {
        List<String> out = new ArrayList<>();
        for (LinkedEntity le : safe(refs)) {
            if (le == null || le.getInstanceId() == null) continue;
            Category category = cast(CategoryAPI.retrieveAPI(EntityNames.CATEGORY.name()).retrieve(le.getInstanceId()), Category.class);
            if (category != null && category.getName() != null) out.add(category.getName());
        }
        return out;
    }

    private static List<String> contactPointEmails(List<LinkedEntity> refs) {
        List<String> out = new ArrayList<>();
        for (LinkedEntity le : safe(refs)) {
            if (le == null || le.getInstanceId() == null) continue;
            ContactPoint cp = cast(ContactPointAPI.retrieveAPI(EntityNames.CONTACTPOINT.name()).retrieve(le.getInstanceId()), ContactPoint.class);
            if (cp == null) continue;
            for (String email : safe(cp.getEmail())) {
                if (email != null && !email.isBlank()) out.add(email);
            }
        }
        return out;
    }

    private static List<String> organizationNames(List<LinkedEntity> refs) {
        List<String> out = new ArrayList<>();
        for (LinkedEntity le : safe(refs)) {
            if (le == null || le.getInstanceId() == null) continue;
            Organization org = cast(OrganizationAPI.retrieveAPI(EntityNames.ORGANIZATION.name()).retrieve(le.getInstanceId()), Organization.class);
            if (org == null) continue;
            for (String name : safe(org.getLegalName())) {
                if (name != null && !name.isBlank()) out.add(name);
            }
        }
        return out;
    }

    private static List<Identifier> resolveIdentifiers(List<LinkedEntity> refs) {
        List<Identifier> out = new ArrayList<>();
        for (LinkedEntity le : safe(refs)) {
            Identifier identifier = resolve(le, Identifier.class);
            if (identifier != null) out.add(identifier);
        }
        return out;
    }

    /** Last valid PeriodOfTime (as in the original code, the last one wins). */
    private static PeriodOfTime lastPeriod(List<LinkedEntity> refs) {
        PeriodOfTime result = null;
        for (LinkedEntity le : safe(refs)) {
            PeriodOfTime pot = resolve(le, PeriodOfTime.class);
            if (pot != null) result = pot;
        }
        return result;
    }

    /** Last valid Location (as in the original code, the last one wins). */
    private static Location lastLocation(List<LinkedEntity> refs) {
        Location result = null;
        for (LinkedEntity le : safe(refs)) {
            Location loc = resolve(le, Location.class);
            if (loc != null) result = loc;
        }
        return result;
    }

    /** Already resolved and validated Distribution data, shared by all export types. */
    private static final class ResolvedDistribution {
        Distribution distribution;
        WebService webService;
        Operation operation;
        List<Mapping> mappings = new ArrayList<>();
        List<String> webServiceContactEmails = new ArrayList<>();
        String providerName;
        String documentationUri;
        List<String> visibleOn;
    }

    private static List<ResolvedDistribution> resolveDistributions(DataProduct dataProduct) {
        List<ResolvedDistribution> out = new ArrayList<>();
        for (LinkedEntity le : safe(dataProduct.getDistribution())) {
            Distribution distribution = resolve(le, Distribution.class);
            if (distribution == null) continue;

            ResolvedDistribution rd = new ResolvedDistribution();
            rd.distribution = distribution;

            rd.webService = resolve(first(distribution.getAccessService()), WebService.class);
            if (rd.webService != null) {
                rd.webServiceContactEmails = contactPointEmails(rd.webService.getContactPoint());
                Organization provider = resolve(rd.webService.getProvider(), Organization.class);
                rd.providerName = provider != null ? first(provider.getLegalName()) : null;
                Documentation documentation = resolve(first(rd.webService.getDocumentation()), Documentation.class);
                rd.documentationUri = documentation != null ? documentation.getUri() : null;
            }

            rd.operation = resolve(first(distribution.getSupportedOperation()), Operation.class);
            if (rd.operation != null) {
                for (LinkedEntity mappingLe : safe(rd.operation.getMapping())) {
                    Mapping mapping = resolve(mappingLe, Mapping.class);
                    if (mapping == null) continue;
                    rd.mappings.add(mapping);
                    for (String val : safe(mapping.getParamValue())) {
                        if (val == null) continue;
                        if (val.contains("covjson") || val.contains("cov+json")) rd.visibleOn = List.of("GRAPH", "MAP");
                        if (val.contains("geojson") || val.contains("geo+json")) rd.visibleOn = List.of("TABLE", "MAP");
                    }
                }
            }
            out.add(rd);
        }
        return out;
    }

    private static String detectDatasetType(DataProduct dataProduct) {
        String keywords = dataProduct.getKeywords();
        if (keywords != null && keywords.contains("pointclick")) return "DYNAMIC";

        String type = null;
        for (LinkedEntity distLe : safe(dataProduct.getDistribution())) {
            if (distLe == null || distLe.getInstanceId() == null) continue;
            Distribution distribution = cast(
                    DistributionAPI.retrieveAPI(EntityNames.DISTRIBUTION.name()).retrieve(distLe.getInstanceId()),
                    Distribution.class);
            if (distribution == null) continue;

            for (LinkedEntity opLe : safe(distribution.getSupportedOperation())) {
                if (opLe == null || opLe.getInstanceId() == null) continue;
                Operation operation = cast(
                        OperationAPI.retrieveAPI(EntityNames.OPERATION.name()).retrieve(opLe.getInstanceId()),
                        Operation.class);
                if (operation == null || operation.getTemplate() == null) continue;

                String template = operation.getTemplate();
                if (template.contains("WMS")) type = "WMS";
                if (template.contains("WFS")) type = "WFS";
                if (template.contains("WMTS")) type = "WMTS";
            }
        }
        return type != null ? type : "HTTP";
    }

    // =====================================================================
    // Public API
    // =====================================================================

    public static JsonArray getListOfObjects(User user) {

        if (!isAdmin(user)) return null;

        JsonArray list = new JsonArray();

        for (Object object : safe(DataProductAPI.retrieveAPI(EntityNames.DATAPRODUCT.name()).retrieveAllWithStatus(StatusType.PUBLISHED))) {
            DataProduct dataProduct = cast(object, DataProduct.class);
            if (dataProduct == null) continue;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid", dataProduct.getInstanceId());
            obj.addProperty("metaid", dataProduct.getMetaId());
            obj.addProperty("type", "Dataset");
            list.add(obj);
        }

        for (Object object : safe(SoftwareApplicationAPI.retrieveAPI(EntityNames.SOFTWAREAPPLICATION.name()).retrieveAllWithStatus(StatusType.PUBLISHED))) {
            SoftwareApplication softwareApplication = cast(object, SoftwareApplication.class);
            if (softwareApplication == null) continue;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid", softwareApplication.getInstanceId());
            obj.addProperty("metaid", softwareApplication.getMetaId());
            obj.addProperty("type", "SoftwareApplication");
            list.add(obj);
        }

        for (Object object : safe(SoftwareSourceCodeAPI.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieveAllWithStatus(StatusType.PUBLISHED))) {
            SoftwareSourceCode softwareSourceCode = cast(object, SoftwareSourceCode.class);
            if (softwareSourceCode == null) continue;
            JsonObject obj = new JsonObject();
            obj.addProperty("instanceid", softwareSourceCode.getInstanceId());
            obj.addProperty("metaid", softwareSourceCode.getMetaId());
            obj.addProperty("type", "SoftwareSourceCode");
            list.add(obj);
        }

        return list;
    }

    public static JsonObject getDataset(String instance_id, User user) {

        if (!isAdmin(user)) return null;

        JsonObject item = new JsonObject();

        DataProduct dataProduct = cast(
                DataProductAPI.retrieveAPI(EntityNames.DATAPRODUCT.name()).retrieve(instance_id),
                DataProduct.class);
        if (dataProduct == null) return item;

        String type = detectDatasetType(dataProduct);

        // Data shared by all types, already resolved and validated
        List<String> categories = categoryNames(dataProduct.getCategory());
        List<String> contactPoints = contactPointEmails(dataProduct.getContactPoint());
        List<String> providers = organizationNames(dataProduct.getPublisher());
        List<String> keywords = splitKeywords(dataProduct.getKeywords());
        List<Identifier> identifiers = resolveIdentifiers(dataProduct.getIdentifier());
        PeriodOfTime period = lastPeriod(dataProduct.getTemporalExtent());
        Location location = lastLocation(dataProduct.getSpatialExtent());
        List<ResolvedDistribution> resolvedDistributions = resolveDistributions(dataProduct);

        if ("DYNAMIC".equals(type)) {
            DynamicDatasetExport dynamicDatasetExport = new DynamicDatasetExport();
            dynamicDatasetExport.setResourceType("dataset");

            DynamicDatasetExport.Metadata metadata = new DynamicDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(str(dataProduct.getCreated().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
            metadata.setDescription(first(dataProduct.getDescription()));
            metadata.setKeywords(keywords);
            List<DynamicDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for (Identifier ide : identifiers) {
                DynamicDatasetExport.PersistentIdentifier pid = new DynamicDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(providers);
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            if (period != null) {
                DynamicDatasetExport.TemporalCoverage gtc = new DynamicDatasetExport.TemporalCoverage();
                gtc.setStart(str(period.getStartDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                gtc.setEnd(str(period.getEndDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(str(dataProduct.getModified().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));

            List<DynamicDatasetExport.Distribution> distributions = new ArrayList<>();
            for (ResolvedDistribution rd : resolvedDistributions) {
                Distribution distribution = rd.distribution;
                DynamicDatasetExport.Distribution dd = new DynamicDatasetExport.Distribution();
                dd.setByteSize(parseLong(str(distribution.getByteSize())));
                dd.setDescription(first(distribution.getDescription()));
                dd.setLicense(distribution.getLicence());
                dd.setTitle(first(distribution.getTitle()));
                if(distribution.getType().contains("WEB_SERVICE")) dd.setType("WEB_SERVICE");
                else {
                    dd.setType("DOWNLOADABLE_FILE");
                    DynamicDatasetExport.DownloadableFile downloadableFile = new DynamicDatasetExport.DownloadableFile();
                    downloadableFile.setFormat(distribution.getFormat());
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if (rd.webService != null || rd.operation != null) {
                    DynamicDatasetExport.WebService webService1 = new DynamicDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(distribution.getFormat());
                    webService1.setProvider(rd.providerName);
                    webService1.setType("DYNAMIC");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = rd.operation.getTemplate().split("\\{?");
                    if (rd.operation != null) {
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<DynamicDatasetExport.Parameter> parameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        DynamicDatasetExport.Parameter parameter = new DynamicDatasetExport.Parameter();
                        boolean multiple = bool(mapping1.getMultipleValues());
                        parameter.setMultipleValues(multiple);
                        parameter.setExplode(multiple);
                        if(mapping1.getDefaultValue()!=null && !mapping1.getDefaultValue().isBlank()) parameter.setDefaultValues(singletonOrEmpty(mapping1.getDefaultValue()));
                        if(mapping1.getParamValue()!=null && !mapping1.getParamValue().isEmpty()) parameter.setEnumValues(new ArrayList<>(safe(mapping1.getParamValue())));
                        parameter.setLabel(mapping1.getLabel());
                        parameter.setName(mapping1.getVariable());
                        if(mapping1.getValuePattern()!=null && !mapping1.getValuePattern().isBlank()) parameter.setPattern(mapping1.getValuePattern());
                        parameter.setPosition(position);
                        if(mapping1.getProperty()!=null && !mapping1.getProperty().isBlank()) {
                            parameter.setProperty(mapping1.getProperty());
                            if(mapping1.getProperty().contains("encodingFormat")) parameter.setRole("FORMAT");
                            if(mapping1.getProperty().contains("startDate")) parameter.setRole("START_DATE");
                            if(mapping1.getProperty().contains("endDate")) parameter.setRole("END_DATE");
                            if(mapping1.getProperty().contains("west")) parameter.setRole("MIN_LON");
                            if(mapping1.getProperty().contains("east")) parameter.setRole("MAX_LON");
                            if(mapping1.getProperty().contains("south")) parameter.setRole("MIN_LAT");
                            if(mapping1.getProperty().contains("north")) parameter.setRole("MAX_LAT");
                        }
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(templateSplit[0].contains(mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        parameters.add(parameter);
                        position++;
                    }
                    webService1.setParameters(parameters);
                    dd.setWebService(webService1);
                }
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            dynamicDatasetExport.setMetadata(metadata);
            item = gson.toJsonTree(dynamicDatasetExport).getAsJsonObject();
        }

        if ("HTTP".equals(type)) {
            HTTPDatasetExport httpDatasetExport = new HTTPDatasetExport();
            httpDatasetExport.setResourceType("dataset");

            HTTPDatasetExport.Metadata metadata = new HTTPDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(str(dataProduct.getCreated().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
            metadata.setDescription(first(dataProduct.getDescription()));
            metadata.setKeywords(keywords);
            List<HTTPDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for (Identifier ide : identifiers) {
                HTTPDatasetExport.PersistentIdentifier pid = new HTTPDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(providers);
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            if (period != null) {
                HTTPDatasetExport.TemporalCoverage gtc = new HTTPDatasetExport.TemporalCoverage();
                gtc.setStart(str(period.getStartDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                gtc.setEnd(str(period.getEndDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(str(dataProduct.getModified().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));

            List<HTTPDatasetExport.Distribution> distributions = new ArrayList<>();
            for (ResolvedDistribution rd : resolvedDistributions) {
                Distribution distribution = rd.distribution;
                HTTPDatasetExport.Distribution dd = new HTTPDatasetExport.Distribution();
                dd.setByteSize(parseLong(str(distribution.getByteSize())));
                dd.setDescription(first(distribution.getDescription()));
                dd.setLicense(distribution.getLicence());
                dd.setTitle(first(distribution.getTitle()));
                if(distribution.getType().contains("WEB_SERVICE")) dd.setType("WEB_SERVICE");
                else {
                    dd.setType("DOWNLOADABLE_FILE");
                    HTTPDatasetExport.DownloadableFile downloadableFile = new HTTPDatasetExport.DownloadableFile();
                    downloadableFile.setFormat(distribution.getFormat());
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if (rd.webService != null || rd.operation != null) {
                    HTTPDatasetExport.WebService webService1 = new HTTPDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(distribution.getFormat());
                    webService1.setProvider(rd.providerName);
                    webService1.setType("HTTP");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = rd.operation.getTemplate().split("\\{?");
                    if (rd.operation != null) {
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<HTTPDatasetExport.Parameter> parameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        HTTPDatasetExport.Parameter parameter = new HTTPDatasetExport.Parameter();
                        boolean multiple = bool(mapping1.getMultipleValues());
                        parameter.setMultipleValues(multiple);
                        parameter.setExplode(multiple);
                        if(mapping1.getDefaultValue()!=null && !mapping1.getDefaultValue().isBlank()) parameter.setDefaultValues(singletonOrEmpty(mapping1.getDefaultValue()));
                        if(mapping1.getParamValue()!=null && !mapping1.getParamValue().isEmpty()) parameter.setEnumValues(new ArrayList<>(safe(mapping1.getParamValue())));
                        parameter.setLabel(mapping1.getLabel());
                        parameter.setName(mapping1.getVariable());
                        if(mapping1.getValuePattern()!=null && !mapping1.getValuePattern().isBlank()) parameter.setPattern(mapping1.getValuePattern());
                        parameter.setPosition(position);
                        if(mapping1.getProperty()!=null && !mapping1.getProperty().isBlank()) {
                            parameter.setProperty(mapping1.getProperty());
                            if(mapping1.getProperty().contains("encodingFormat")) parameter.setRole("FORMAT");
                            if(mapping1.getProperty().contains("startDate")) parameter.setRole("START_DATE");
                            if(mapping1.getProperty().contains("endDate")) parameter.setRole("END_DATE");
                            if(mapping1.getProperty().contains("west")) parameter.setRole("MIN_LON");
                            if(mapping1.getProperty().contains("east")) parameter.setRole("MAX_LON");
                            if(mapping1.getProperty().contains("south")) parameter.setRole("MIN_LAT");
                            if(mapping1.getProperty().contains("north")) parameter.setRole("MAX_LAT");
                        }
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(templateSplit[0].contains(mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        parameters.add(parameter);
                        position++;
                    }
                    webService1.setParameters(parameters);
                    dd.setWebService(webService1);
                }
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            httpDatasetExport.setMetadata(metadata);
            item = gson.toJsonTree(httpDatasetExport).getAsJsonObject();
        }

        if ("WMS".equals(type)) {
            WMSDatasetExport wmsDatasetExport = new WMSDatasetExport();
            wmsDatasetExport.setResourceType("dataset");

            WMSDatasetExport.Metadata metadata = new WMSDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(str(dataProduct.getCreated().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
            metadata.setDescription(first(dataProduct.getDescription()));
            metadata.setKeywords(keywords);
            List<WMSDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for (Identifier ide : identifiers) {
                WMSDatasetExport.PersistentIdentifier pid = new WMSDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(providers);
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            if (period != null) {
                WMSDatasetExport.TemporalCoverage gtc = new WMSDatasetExport.TemporalCoverage();
                gtc.setStart(str(period.getStartDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                gtc.setEnd(str(period.getEndDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(str(dataProduct.getModified().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));

            List<WMSDatasetExport.Distribution> distributions = new ArrayList<>();
            for (ResolvedDistribution rd : resolvedDistributions) {
                Distribution distribution = rd.distribution;
                WMSDatasetExport.Distribution dd = new WMSDatasetExport.Distribution();
                dd.setByteSize(parseLong(str(distribution.getByteSize())));
                dd.setDescription(first(distribution.getDescription()));
                dd.setLicense(distribution.getLicence());
                dd.setTitle(first(distribution.getTitle()));
                if(distribution.getType().contains("WEB_SERVICE")) dd.setType("WEB_SERVICE");
                else {
                    dd.setType("DOWNLOADABLE_FILE");
                    WMSDatasetExport.DownloadableFile downloadableFile = new WMSDatasetExport.DownloadableFile();
                    downloadableFile.setFormat(distribution.getFormat());
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if (rd.webService != null || rd.operation != null) {
                    WMSDatasetExport.WebService webService1 = new WMSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(distribution.getFormat());
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WMS");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = rd.operation.getTemplate().split("\\{?");
                    if (rd.operation != null) {
                        webService1.setCapabilitiesUrl(rd.operation.getTemplate());
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WMSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        WMSDatasetExport.Parameter parameter = new WMSDatasetExport.Parameter();
                        boolean multiple = bool(mapping1.getMultipleValues());
                        parameter.setMultipleValues(multiple);
                        parameter.setExplode(multiple);
                        if(mapping1.getDefaultValue()!=null && !mapping1.getDefaultValue().isBlank()) parameter.setDefaultValues(singletonOrEmpty(mapping1.getDefaultValue()));
                        if(mapping1.getParamValue()!=null && !mapping1.getParamValue().isEmpty()) parameter.setEnumValues(new ArrayList<>(safe(mapping1.getParamValue())));
                        parameter.setLabel(mapping1.getLabel());
                        parameter.setName(mapping1.getVariable());
                        if(mapping1.getValuePattern()!=null && !mapping1.getValuePattern().isBlank()) parameter.setPattern(mapping1.getValuePattern());
                        parameter.setPosition(position);
                        if(mapping1.getProperty()!=null && !mapping1.getProperty().isBlank()) {
                            parameter.setProperty(mapping1.getProperty());
                            if(mapping1.getProperty().contains("encodingFormat")) parameter.setRole("FORMAT");
                            if(mapping1.getProperty().contains("startDate")) parameter.setRole("START_DATE");
                            if(mapping1.getProperty().contains("endDate")) parameter.setRole("END_DATE");
                            if(mapping1.getProperty().contains("west")) parameter.setRole("MIN_LON");
                            if(mapping1.getProperty().contains("east")) parameter.setRole("MAX_LON");
                            if(mapping1.getProperty().contains("south")) parameter.setRole("MIN_LAT");
                            if(mapping1.getProperty().contains("north")) parameter.setRole("MAX_LAT");
                        }
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(templateSplit[0].contains(mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        mapParameters.add(parameter);
                    }
                    webService1.setGetMapParameters(mapParameters);
                    dd.setWebService(webService1);
                }
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            wmsDatasetExport.setMetadata(metadata);
            item = gson.toJsonTree(wmsDatasetExport).getAsJsonObject();
        }

        if ("WMTS".equals(type)) {
            WMTSDatasetExport wmtsDatasetExport = new WMTSDatasetExport();
            wmtsDatasetExport.setResourceType("dataset");

            WMTSDatasetExport.Metadata metadata = new WMTSDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(str(dataProduct.getCreated().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
            metadata.setDescription(first(dataProduct.getDescription()));
            metadata.setKeywords(keywords);
            List<WMTSDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for (Identifier ide : identifiers) {
                WMTSDatasetExport.PersistentIdentifier pid = new WMTSDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(providers);
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            if (period != null) {
                WMTSDatasetExport.TemporalCoverage gtc = new WMTSDatasetExport.TemporalCoverage();
                gtc.setStart(str(period.getStartDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                gtc.setEnd(str(period.getEndDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(str(dataProduct.getModified().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));

            List<WMTSDatasetExport.Distribution> distributions = new ArrayList<>();
            for (ResolvedDistribution rd : resolvedDistributions) {
                Distribution distribution = rd.distribution;
                WMTSDatasetExport.Distribution dd = new WMTSDatasetExport.Distribution();
                dd.setByteSize(parseLong(str(distribution.getByteSize())));
                dd.setDescription(first(distribution.getDescription()));
                dd.setLicense(distribution.getLicence());
                dd.setTitle(first(distribution.getTitle()));
                if(distribution.getType().contains("WEB_SERVICE")) dd.setType("WEB_SERVICE");
                else {
                    dd.setType("DOWNLOADABLE_FILE");
                    WMTSDatasetExport.DownloadableFile downloadableFile = new WMTSDatasetExport.DownloadableFile();
                    downloadableFile.setFormat(distribution.getFormat());
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if (rd.webService != null || rd.operation != null) {
                    WMTSDatasetExport.WebService webService1 = new WMTSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(distribution.getFormat());
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WMTS");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = rd.operation.getTemplate().split("\\{?");
                    if (rd.operation != null) {
                        webService1.setCapabilitiesUrl(rd.operation.getTemplate());
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WMTSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        WMTSDatasetExport.Parameter parameter = new WMTSDatasetExport.Parameter();
                        boolean multiple = bool(mapping1.getMultipleValues());
                        parameter.setMultipleValues(multiple);
                        parameter.setExplode(multiple);
                        if(mapping1.getDefaultValue()!=null && !mapping1.getDefaultValue().isBlank()) parameter.setDefaultValues(singletonOrEmpty(mapping1.getDefaultValue()));
                        if(mapping1.getParamValue()!=null && !mapping1.getParamValue().isEmpty()) parameter.setEnumValues(new ArrayList<>(safe(mapping1.getParamValue())));
                        parameter.setLabel(mapping1.getLabel());
                        parameter.setName(mapping1.getVariable());
                        if(mapping1.getValuePattern()!=null && !mapping1.getValuePattern().isBlank()) parameter.setPattern(mapping1.getValuePattern());
                        parameter.setPosition(position);
                        if(mapping1.getProperty()!=null && !mapping1.getProperty().isBlank()) {
                            parameter.setProperty(mapping1.getProperty());
                            if(mapping1.getProperty().contains("encodingFormat")) parameter.setRole("FORMAT");
                            if(mapping1.getProperty().contains("startDate")) parameter.setRole("START_DATE");
                            if(mapping1.getProperty().contains("endDate")) parameter.setRole("END_DATE");
                            if(mapping1.getProperty().contains("west")) parameter.setRole("MIN_LON");
                            if(mapping1.getProperty().contains("east")) parameter.setRole("MAX_LON");
                            if(mapping1.getProperty().contains("south")) parameter.setRole("MIN_LAT");
                            if(mapping1.getProperty().contains("north")) parameter.setRole("MAX_LAT");
                        }
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(templateSplit[0].contains(mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        mapParameters.add(parameter);
                        position++;
                    }
                    webService1.setGetMapParameters(mapParameters);
                    dd.setWebService(webService1);
                }
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            wmtsDatasetExport.setMetadata(metadata);
            item = gson.toJsonTree(wmtsDatasetExport).getAsJsonObject();
        }

        if ("WFS".equals(type)) {
            WFSDatasetExport wfsDatasetExport = new WFSDatasetExport();
            wfsDatasetExport.setResourceType("dataset");

            WFSDatasetExport.Metadata metadata = new WFSDatasetExport.Metadata();
            metadata.setAccessRight(dataProduct.getAccessRight());
            metadata.setAccrualPeriodicity(dataProduct.getAccrualPeriodicity());
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(str(dataProduct.getCreated().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
            metadata.setDescription(first(dataProduct.getDescription()));
            metadata.setKeywords(keywords);
            List<WFSDatasetExport.PersistentIdentifier> listIdentifiers = new ArrayList<>();
            for (Identifier ide : identifiers) {
                WFSDatasetExport.PersistentIdentifier pid = new WFSDatasetExport.PersistentIdentifier();
                pid.setType(ide.getType());
                pid.setValue(ide.getIdentifier());
                listIdentifiers.add(pid);
            }
            metadata.setPersistentIdentifiers(listIdentifiers);
            metadata.setProviders(providers);
            metadata.setQualityAnnotation(dataProduct.getQualityAssurance());
            if (period != null) {
                WFSDatasetExport.TemporalCoverage gtc = new WFSDatasetExport.TemporalCoverage();
                gtc.setStart(str(period.getStartDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                gtc.setEnd(str(period.getEndDate().atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(str(dataProduct.getModified().atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))));

            List<WFSDatasetExport.Distribution> distributions = new ArrayList<>();
            for (ResolvedDistribution rd : resolvedDistributions) {
                Distribution distribution = rd.distribution;
                WFSDatasetExport.Distribution dd = new WFSDatasetExport.Distribution();
                dd.setByteSize(parseLong(str(distribution.getByteSize())));
                dd.setDescription(first(distribution.getDescription()));
                dd.setLicense(distribution.getLicence());
                dd.setTitle(first(distribution.getTitle()));
                if(distribution.getType().contains("WEB_SERVICE")) dd.setType("WEB_SERVICE");
                else {
                    dd.setType("DOWNLOADABLE_FILE");
                    WFSDatasetExport.DownloadableFile downloadableFile = new WFSDatasetExport.DownloadableFile();
                    downloadableFile.setFormat(distribution.getFormat());
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if (rd.webService != null || rd.operation != null) {
                    WFSDatasetExport.WebService webService1 = new WFSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(distribution.getFormat());
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WFS"); // was "WMS" in the original code
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = rd.operation.getTemplate().split("\\{?");
                    if (rd.operation != null) {
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WFSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        WFSDatasetExport.Parameter parameter = new WFSDatasetExport.Parameter();
                        boolean multiple = bool(mapping1.getMultipleValues());
                        parameter.setMultipleValues(multiple);
                        parameter.setExplode(multiple);
                        if(mapping1.getDefaultValue()!=null && !mapping1.getDefaultValue().isBlank()) parameter.setDefaultValues(singletonOrEmpty(mapping1.getDefaultValue()));
                        if(mapping1.getParamValue()!=null && !mapping1.getParamValue().isEmpty()) parameter.setEnumValues(new ArrayList<>(safe(mapping1.getParamValue())));
                        parameter.setLabel(mapping1.getLabel());
                        parameter.setName(mapping1.getVariable());
                        if(mapping1.getValuePattern()!=null && !mapping1.getValuePattern().isBlank()) parameter.setPattern(mapping1.getValuePattern());
                        parameter.setPosition(position);
                        if(mapping1.getProperty()!=null && !mapping1.getProperty().isBlank()) {
                            parameter.setProperty(mapping1.getProperty());
                            if(mapping1.getProperty().contains("encodingFormat")) parameter.setRole("FORMAT");
                            if(mapping1.getProperty().contains("startDate")) parameter.setRole("START_DATE");
                            if(mapping1.getProperty().contains("endDate")) parameter.setRole("END_DATE");
                            if(mapping1.getProperty().contains("west")) parameter.setRole("MIN_LON");
                            if(mapping1.getProperty().contains("east")) parameter.setRole("MAX_LON");
                            if(mapping1.getProperty().contains("south")) parameter.setRole("MIN_LAT");
                            if(mapping1.getProperty().contains("north")) parameter.setRole("MAX_LAT");
                        }
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(templateSplit[0].contains(mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        mapParameters.add(parameter);
                        position++;
                    }
                    webService1.setParameters(mapParameters);
                    dd.setWebService(webService1);
                }
                distributions.add(dd);
            }
            metadata.setDistributions(distributions);

            wfsDatasetExport.setMetadata(metadata);
            item = gson.toJsonTree(wfsDatasetExport).getAsJsonObject();
        }

        return item;
    }

    public static JsonObject getSoftwareApplication(String instance_id, User user) {

        if (!isAdmin(user)) return null;

        JsonObject item = new JsonObject();

        SoftwareApplication softwareApplication = cast(
                SoftwareApplicationAPI.retrieveAPI(EntityNames.SOFTWAREAPPLICATION.name()).retrieve(instance_id),
                SoftwareApplication.class);
        if (softwareApplication == null) return item;

        SoftwareApplicationExport softwareApplicationExport = new SoftwareApplicationExport();
        softwareApplicationExport.setResourceType("software-application");

        SoftwareApplicationExport.Metadata metadata = new SoftwareApplicationExport.Metadata();
        // TODO: categories and creators are read from getContactPoint(): getCategory()
        //       and the author/publisher field should probably be used instead.
        metadata.setCategories(categoryNames(softwareApplication.getContactPoint()));
        metadata.setContactPoints(contactPointEmails(softwareApplication.getContactPoint()));
        metadata.setCreators(organizationNames(softwareApplication.getContactPoint()));
        metadata.setDescription(softwareApplication.getDescription());
        metadata.setDownloadUrl(softwareApplication.getDownloadURL());
        List<SoftwareApplicationExport.Identifier> identifiers = new ArrayList<>();
        for (Identifier identifier : resolveIdentifiers(softwareApplication.getIdentifier())) {
            SoftwareApplicationExport.Identifier identifier1 = new SoftwareApplicationExport.Identifier();
            identifier1.setType(identifier.getType());
            identifier1.setValue(identifier.getIdentifier());
            identifiers.add(identifier1);
        }
        metadata.setIdentifiers(identifiers);
        metadata.setKeywords(splitKeywords(softwareApplication.getKeywords()));
        metadata.setLicense(softwareApplication.getLicenseURL());
        metadata.setMainEntityOfPage(softwareApplication.getMainEntityOfPage());
        metadata.setName(softwareApplication.getName());
        metadata.setOperatingSystem(first(softwareApplication.getOperatingSystem()));
        metadata.setRequirements(softwareApplication.getRequirements());
        metadata.setVersion(softwareApplication.getVersion());

        softwareApplicationExport.setMetadata(metadata);

        item = gson.toJsonTree(softwareApplicationExport).getAsJsonObject();

        return item;
    }

    public static JsonObject getSoftwareSourceCode(String instance_id, User user) {

        if (!isAdmin(user)) return null;

        JsonObject item = new JsonObject();

        SoftwareSourceCode softwareSourceCode = cast(
                SoftwareSourceCodeAPI.retrieveAPI(EntityNames.SOFTWARESOURCECODE.name()).retrieve(instance_id),
                SoftwareSourceCode.class);
        if (softwareSourceCode == null) return item;

        SoftwareSourceCodeExport softwareSourceCodeExport = new SoftwareSourceCodeExport();
        softwareSourceCodeExport.setResourceType("software-source-code");

        SoftwareSourceCodeExport.Metadata metadata = new SoftwareSourceCodeExport.Metadata();
        // TODO: same as above, categories and creators are read from getContactPoint().
        metadata.setCategories(categoryNames(softwareSourceCode.getContactPoint()));
        metadata.setContactPoints(contactPointEmails(softwareSourceCode.getContactPoint()));
        metadata.setCreators(organizationNames(softwareSourceCode.getContactPoint()));
        metadata.setDescription(softwareSourceCode.getDescription());
        metadata.setRuntimePlatform(softwareSourceCode.getRuntimePlatform());
        List<SoftwareSourceCodeExport.Identifier> identifiers = new ArrayList<>();
        for (Identifier identifier : resolveIdentifiers(softwareSourceCode.getIdentifier())) {
            SoftwareSourceCodeExport.Identifier identifier1 = new SoftwareSourceCodeExport.Identifier();
            identifier1.setType(identifier.getType());
            identifier1.setValue(identifier.getIdentifier());
            identifiers.add(identifier1);
        }
        metadata.setIdentifiers(identifiers);
        metadata.setKeywords(splitKeywords(softwareSourceCode.getKeywords()));
        metadata.setLicense(softwareSourceCode.getLicenseURL());
        metadata.setMainEntityOfPage(softwareSourceCode.getMainEntityofPage());
        metadata.setName(softwareSourceCode.getName());
        metadata.setProgrammingLanguage(first(softwareSourceCode.getProgrammingLanguage()));
        metadata.setRepository(softwareSourceCode.getCodeRepository());
        metadata.setVersion(softwareSourceCode.getVersion());

        softwareSourceCodeExport.setMetadata(metadata);

        item = gson.toJsonTree(softwareSourceCodeExport).getAsJsonObject();

        return item;
    }
}