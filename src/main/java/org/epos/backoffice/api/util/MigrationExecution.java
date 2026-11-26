package org.epos.backoffice.api.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import commonapis.LinkedEntityAPI;
import metadataapis.*;
import model.StatusType;
import org.epos.backoffice.bean.*;
import org.epos.eposdatamodel.*;

import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.math.BigDecimal;
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

    private static String timestamp(LocalDateTime value) {
        return value == null ? null : value.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
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
        return Arrays.stream(keywords.replace("/"," ").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static String templateBase(Operation operation) {
        if (operation == null || operation.getTemplate() == null || operation.getTemplate().isBlank()) {
            throw new IllegalArgumentException("A web service distribution requires an operation with a template URL");
        }
        return operation.getTemplate().split("\\{\\?", 2)[0];
    }

    private static boolean isPathParameter(String template, String variable) {
        return variable != null && template.contains("{" + variable + "}");
    }

    static String dateTimeLayout(String pattern) {
        return switch (pattern.trim()) {
            case "yyyy-MM-dd", "YYYY-MM-DD" -> "YYYY-MM-DD";
            case "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-ddTHH:mm:ss", "YYYY-MM-DDThh:mm:ss" -> "YYYY-MM-DDThh:mm:ss";
            case "yyyy-MM-dd HH:mm:ss", "YYYY-MM-DD hh:mm:ss" -> "YYYY-MM-DD hh:mm:ss";
            case "yyyy-MM-dd'T'HH:mm:ss'Z'", "YYYY-MM-DDThh:mm:ssZ" -> "YYYY-MM-DDThh:mm:ssZ";
            default -> throw new IllegalArgumentException("Unsupported legacy DATETIME pattern: " + pattern);
        };
    }

    private static Mapping findMapping(List<Mapping> mappings, String... names) {
        for (Mapping mapping : mappings) {
            for (String name : names) {
                if (name.equalsIgnoreCase(mapping.getVariable())) return mapping;
            }
        }
        return null;
    }

    private static Mapping uniqueMapping(List<Mapping> mappings, String... names) {
        Mapping found = null;
        for (Mapping mapping : mappings) {
            for (String name : names) {
                if (name.equalsIgnoreCase(mapping.getVariable())) {
                    if (found != null && found != mapping) throw new IllegalArgumentException("Ambiguous coordinate mapping: " + String.join(", ", names));
                    found = mapping;
                    break;
                }
            }
        }
        return found;
    }

    static List<String> mappingChoices(Mapping mapping, String field) {
        if (mapping == null) throw new IllegalArgumentException("Required mapping missing: " + field);
        List<String> choices = safe(mapping.getParamValue()).stream()
                .filter(value -> value != null && !value.isBlank())
                .map(String::trim).distinct().collect(Collectors.toCollection(ArrayList::new));
        String selected = mapping.getDefaultValue();
        if (selected != null && !selected.isBlank() && !choices.contains(selected.trim())) choices.add(selected.trim());
        if (choices.isEmpty()) throw new IllegalArgumentException("Mapping has no choices: " + field);
        return choices;
    }

    static String mappingDefault(Mapping mapping, List<String> choices, String field) {
        String selected = mapping.getDefaultValue();
        if (selected != null && !selected.isBlank()) return selected.trim();
        if (choices.size() == 1) return choices.get(0);
        throw new IllegalArgumentException("Mapping has multiple choices without a default: " + field);
    }

    private static boolean isWfsReservedParameter(String name) {
        return name != null && List.of("SERVICE", "REQUEST", "VERSION", "ACCEPTVERSIONS", "ACCEPTFORMATS", "SECTIONS", "UPDATESEQUENCE", "TYPENAME", "TYPENAMES", "OUTPUTFORMAT", "MAXFEATURES", "COUNT", "BBOX", "SRSNAME", "PROPERTYNAME", "FEATUREVERSION", "FEATUREID", "RESOURCEID", "FILTER", "FILTER_LANGUAGE", "SORTBY", "RESULTTYPE", "STARTINDEX", "NAMESPACE", "NAMESPACES", "ALIASES", "RESOLVE", "RESOLVEDEPTH", "RESOLVETIMEOUT", "STOREDQUERY_ID").contains(name.toUpperCase(java.util.Locale.ROOT));
    }

    private static boolean isOgcMapReservedParameter(String name, boolean tile) {
        if (name == null) return false;
        String upper = name.toUpperCase(java.util.Locale.ROOT);
        if (tile) return List.of("SERVICE", "REQUEST", "VERSION", "LAYER", "STYLE", "FORMAT", "TILEMATRIXSET", "TILEMATRIX", "TILEROW", "TILECOL").contains(upper);
        return List.of("SERVICE", "VERSION", "REQUEST", "LAYERS", "STYLES", "SRS", "CRS", "BBOX", "WIDTH", "HEIGHT", "FORMAT", "TRANSPARENT", "BGCOLOR", "EXCEPTIONS").contains(upper);
    }

    // Legacy mappings carry a property URI and string enum values. The draft API
    // represents the former as a role and expects enum options as objects.
    static void normalizeDraftParameters(JsonObject item) {
        JsonObject metadata = item.has("metadata") && item.get("metadata").isJsonObject()
                ? item.getAsJsonObject("metadata") : null;
        if (metadata == null || !metadata.has("distributions") || !metadata.get("distributions").isJsonArray()) return;
        for (JsonElement distribution : metadata.getAsJsonArray("distributions")) {
            if (!distribution.isJsonObject()) continue;
            JsonObject object = distribution.getAsJsonObject();
            if (!object.has("web_service") || !object.get("web_service").isJsonObject()) continue;
            JsonObject service = object.getAsJsonObject("web_service");
            for (String field : List.of("parameters", "body_parameters", "get_map_parameters", "get_tile_parameters", "get_feature_info_parameters")) {
                if (!service.has(field) || !service.get(field).isJsonArray()) continue;
                normalizeParameterArray(service.getAsJsonArray(field));
            }
        }
    }

    private static void normalizeParameterArray(JsonArray parameters) {
        for (JsonElement element : parameters) {
            if (!element.isJsonObject()) continue;
            JsonObject parameter = element.getAsJsonObject();
            parameter.remove("property");
            String role = parameter.has("role") ? parameter.get("role").getAsString() : "";
            boolean dynamicCoordinate = List.of("LONGITUDE", "LATITUDE").contains(role);
            boolean coordinate = dynamicCoordinate || List.of("MIN_LON", "MAX_LON", "MIN_LAT", "MAX_LAT").contains(role);
            boolean date = List.of("START_DATE", "END_DATE").contains(role);
            if (coordinate) {
                if (dynamicCoordinate && (!parameter.has("required") || !parameter.get("required").getAsBoolean()
                        || !parameter.has("read_only") || parameter.get("read_only").getAsBoolean()
                        || (parameter.has("explode") && parameter.get("explode").getAsBoolean())
                        || (parameter.has("default") && !parameter.getAsJsonArray("default").isEmpty())
                        || (parameter.has("enum") && !parameter.getAsJsonArray("enum").isEmpty()))) {
                    throw new IllegalArgumentException("DYNAMIC coordinate must be required, editable, single-valued and have no default or enum: " + parameter.get("name"));
                }
                if (parameter.has("multiple_values") && parameter.get("multiple_values").getAsBoolean()) {
                    throw new IllegalArgumentException("Coordinate parameter must be single-valued: " + parameter.get("name"));
                }
                parameter.addProperty("kind", "FLOAT");
                parameter.remove("pattern");
                if (parameter.has("default")) {
                    JsonArray values = new JsonArray();
                    for (JsonElement value : parameter.getAsJsonArray("default")) {
                        values.add(new JsonPrimitive(new BigDecimal(value.getAsString())));
                    }
                    parameter.add("default", values);
                }
            } else if (date) {
                parameter.addProperty("kind", "DATETIME");
                parameter.remove("multiple_values");
                if (parameter.has("pattern")) {
                    String pattern = parameter.remove("pattern").getAsString();
                    try {
                        parameter.addProperty("layout", dateTimeLayout(pattern));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("DATETIME parameter " + parameter.get("name") + ": " + e.getMessage(), e);
                    }
                }
                if (parameter.has("enum") && !parameter.getAsJsonArray("enum").isEmpty()) {
                    throw new IllegalArgumentException("DATETIME parameter cannot represent enum values: " + parameter.get("name"));
                }
                parameter.remove("enum");
                if (parameter.has("default")) {
                    JsonArray values = parameter.getAsJsonArray("default");
                    if (values.size() != 1) throw new IllegalArgumentException("DATETIME default must contain one value: " + parameter.get("name"));
                    parameter.add("default", values.get(0).deepCopy());
                }
            }
            if (parameter.has("enum") && parameter.get("enum").isJsonArray()) {
                JsonArray options = new JsonArray();
                for (JsonElement value : parameter.getAsJsonArray("enum")) {
                    if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
                        JsonObject option = new JsonObject();
                        option.add("value", coordinate ? new JsonPrimitive(new BigDecimal(value.getAsString())) : value.deepCopy());
                        options.add(option);
                    } else {
                        options.add(value.deepCopy());
                    }
                }
                parameter.add("enum", options);
            }
            if (parameter.has("parameters") && parameter.get("parameters").isJsonArray()) {
                normalizeParameterArray(parameter.getAsJsonArray("parameters"));
            }
        }
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
            metadata.setAccessRight(AccessRightMapper.toEuCode(dataProduct.getAccessRight()));
            metadata.setAccrualPeriodicity(FrequencyMapper.toEuCode(dataProduct.getAccrualPeriodicity()));
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(timestamp(dataProduct.getCreated()));
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
                gtc.setStart(timestamp(period.getStartDate()));
                gtc.setEnd(timestamp(period.getEndDate()));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(timestamp(dataProduct.getModified()));

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
                    downloadableFile.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if ("WEB_SERVICE".equals(dd.getType())) {
                    DynamicDatasetExport.WebService webService1 = new DynamicDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    webService1.setProvider(rd.providerName);
                    webService1.setType("DYNAMIC");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = {templateBase(rd.operation)};
                    if (rd.operation != null) {
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<DynamicDatasetExport.Parameter> parameters = new ArrayList<>();
                    Mapping longitude = uniqueMapping(rd.mappings, "longitude", "lon");
                    Mapping latitude = uniqueMapping(rd.mappings, "latitude", "lat");
                    if (longitude == null || latitude == null) {
                        throw new IllegalArgumentException("DYNAMIC service requires longitude and latitude mappings");
                    }
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
                        if (mapping1 == longitude) parameter.setRole("LONGITUDE");
                        if (mapping1 == latitude) parameter.setRole("LATITUDE");
                        parameter.setRequired(bool(mapping1.getRequired()));
                        parameter.setReadOnly(bool(mapping1.getReadOnlyValue()));
                        if(isPathParameter(templateSplit[0], mapping1.getVariable()))
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
            metadata.setAccessRight(AccessRightMapper.toEuCode(dataProduct.getAccessRight()));
            metadata.setAccrualPeriodicity(FrequencyMapper.toEuCode(dataProduct.getAccrualPeriodicity()));
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(timestamp(dataProduct.getCreated()));
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
                gtc.setStart(timestamp(period.getStartDate()));
                gtc.setEnd(timestamp(period.getEndDate()));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(timestamp(dataProduct.getModified()));

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
                    downloadableFile.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if ("WEB_SERVICE".equals(dd.getType())) {
                    HTTPDatasetExport.WebService webService1 = new HTTPDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    webService1.setProvider(rd.providerName);
                    webService1.setType("HTTP");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = {templateBase(rd.operation)};
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
                        if(isPathParameter(templateSplit[0], mapping1.getVariable()))
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
            metadata.setAccessRight(AccessRightMapper.toEuCode(dataProduct.getAccessRight()));
            metadata.setAccrualPeriodicity(FrequencyMapper.toEuCode(dataProduct.getAccrualPeriodicity()));
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(timestamp(dataProduct.getCreated()));
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
                gtc.setStart(timestamp(period.getStartDate()));
                gtc.setEnd(timestamp(period.getEndDate()));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(timestamp(dataProduct.getModified()));

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
                    downloadableFile.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if ("WEB_SERVICE".equals(dd.getType())) {
                    WMSDatasetExport.WebService webService1 = new WMSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WMS");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = {templateBase(rd.operation)};
                    if (rd.operation != null) {
                        webService1.setCapabilitiesUrl(templateSplit[0]);
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    Mapping layerMapping = findMapping(rd.mappings, "layers", "layer");
                    List<String> layerNames = mappingChoices(layerMapping, "WMS layers");
                    webService1.setDefaultLayer(mappingDefault(layerMapping, layerNames, "WMS layers"));
                    Mapping styleMapping = findMapping(rd.mappings, "styles", "style");
                    List<String> styles = styleMapping == null ? List.of("") : mappingChoices(styleMapping, "WMS styles");
                    String defaultStyle = styleMapping == null ? "" : mappingDefault(styleMapping, styles, "WMS styles");
                    if (layerNames.size() > 1 && styleMapping != null) {
                        throw new IllegalArgumentException("Cannot associate WMS styles with multiple layers from a single mapping");
                    }
                    List<WMSDatasetExport.Layer> layers = new ArrayList<>();
                    for (String name : layerNames) {
                        WMSDatasetExport.Layer layer = new WMSDatasetExport.Layer();
                        layer.setName(name);
                        layer.setStyles(styles);
                        layer.setDefaultStyle(defaultStyle);
                        layers.add(layer);
                    }
                    webService1.setLayers(layers);
                    Mapping transparency = findMapping(rd.mappings, "transparent");
                    webService1.setTransparencyEditable(transparency != null && !bool(transparency.getReadOnlyValue()));
                    webService1.setDefaultTransparency(transparency != null && bool(transparency.getDefaultValue()));
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WMSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        if (isOgcMapReservedParameter(mapping1.getVariable(), false)) continue;
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
                        if(isPathParameter(templateSplit[0], mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        mapParameters.add(parameter);
                        position++;
                    }
                    webService1.setGetMapParameters(mapParameters);
                    webService1.setGetFeatureInfoParameters(new ArrayList<>());
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
            metadata.setAccessRight(AccessRightMapper.toEuCode(dataProduct.getAccessRight()));
            metadata.setAccrualPeriodicity(FrequencyMapper.toEuCode(dataProduct.getAccrualPeriodicity()));
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(timestamp(dataProduct.getCreated()));
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
                gtc.setStart(timestamp(period.getStartDate()));
                gtc.setEnd(timestamp(period.getEndDate()));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(timestamp(dataProduct.getModified()));

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
                    downloadableFile.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if ("WEB_SERVICE".equals(dd.getType())) {
                    WMTSDatasetExport.WebService webService1 = new WMTSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WMTS");
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = {templateBase(rd.operation)};
                    if (rd.operation != null) {
                        webService1.setCapabilitiesUrl(templateSplit[0]);
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                    }
                    Mapping layerMapping = findMapping(rd.mappings, "layer");
                    List<String> layerNames = mappingChoices(layerMapping, "WMTS layers");
                    webService1.setDefaultLayer(mappingDefault(layerMapping, layerNames, "WMTS layers"));
                    Mapping styleMapping = findMapping(rd.mappings, "style");
                    if (layerNames.size() > 1 && styleMapping != null) {
                        throw new IllegalArgumentException("Cannot associate WMTS styles with multiple layers from a single mapping");
                    }
                    List<String> styles = styleMapping == null ? null : mappingChoices(styleMapping, "WMTS styles");
                    String defaultStyle = styleMapping == null ? null : mappingDefault(styleMapping, styles, "WMTS styles");
                    List<WMTSDatasetExport.Layer> layers = new ArrayList<>();
                    for (String name : layerNames) {
                        WMTSDatasetExport.Layer layer = new WMTSDatasetExport.Layer();
                        layer.setName(name);
                        layer.setStyles(styles);
                        layer.setDefaultStyle(defaultStyle);
                        layers.add(layer);
                    }
                    webService1.setLayers(layers);
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WMTSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        if (isOgcMapReservedParameter(mapping1.getVariable(), true)) continue;
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
                        if(isPathParameter(templateSplit[0], mapping1.getVariable()))
                            parameter.setLocation("PATH");
                        else parameter.setLocation("QUERY");
                        parameter.setKind("STRING");
                        mapParameters.add(parameter);
                        position++;
                    }
                    webService1.setGetTileParameters(mapParameters);
                    webService1.setGetFeatureInfoParameters(new ArrayList<>());
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
            metadata.setAccessRight(AccessRightMapper.toEuCode(dataProduct.getAccessRight()));
            metadata.setAccrualPeriodicity(FrequencyMapper.toEuCode(dataProduct.getAccrualPeriodicity()));
            metadata.setCategories(categories);
            metadata.setContactPoints(contactPoints);
            metadata.setCreated(timestamp(dataProduct.getCreated()));
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
                gtc.setStart(timestamp(period.getStartDate()));
                gtc.setEnd(timestamp(period.getEndDate()));
                metadata.setTemporalCoverage(gtc);
            }
            if (location != null) metadata.setSpatialCoverage(location.getLocation());
            metadata.setTitle(first(dataProduct.getTitle()));
            metadata.setUpdated(timestamp(dataProduct.getModified()));

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
                    downloadableFile.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    downloadableFile.setUrl(first(distribution.getDownloadURL()));
                    dd.setDownloadableFile(downloadableFile);
                }

                if ("WEB_SERVICE".equals(dd.getType())) {
                    WFSDatasetExport.WebService webService1 = new WFSDatasetExport.WebService();
                    webService1.setContactPoints(rd.webServiceContactEmails);
                    webService1.setFormat(FileTypeMapper.toMimeType(distribution.getFormat()));
                    webService1.setProvider(rd.providerName);
                    webService1.setType("WFS"); // was "WMS" in the original code
                    webService1.setDocumentationUri(rd.documentationUri);
                    String[] templateSplit = {templateBase(rd.operation)};
                    if (rd.operation != null) {
                        webService1.setMethod(rd.operation.getMethod());
                        webService1.setTemplateUrl(templateSplit[0]);
                        webService1.setGetFeatureTemplateUrl(templateSplit[0]);
                    }
                    Mapping versionMapping = findMapping(rd.mappings, "version");
                    List<String> versions = mappingChoices(versionMapping, "version");
                    String version = mappingDefault(versionMapping, versions, "version");
                    if (!List.of("1.1.0", "2.0.0").contains(version)) {
                        throw new IllegalArgumentException("Unsupported WFS version: " + version);
                    }
                    webService1.setVersion(version);
                    Mapping featureMapping = findMapping(rd.mappings, "typenames", "typename");
                    List<String> featureTypes = mappingChoices(featureMapping, "feature_types");
                    webService1.setFeatureTypes(featureTypes);
                    webService1.setDefaultFeatureType(mappingDefault(featureMapping, featureTypes, "feature_types"));
                    Mapping formatMapping = findMapping(rd.mappings, "outputformat");
                    List<String> outputFormats = mappingChoices(formatMapping, "output_formats");
                    webService1.setOutputFormats(outputFormats);
                    webService1.setDefaultOutputFormat(mappingDefault(formatMapping, outputFormats, "output_formats"));
                    Mapping limitMapping = findMapping(rd.mappings, "count", "maxfeatures");
                    webService1.setResultLimitEditable(limitMapping != null && !bool(limitMapping.getReadOnlyValue()));
                    if (limitMapping != null && limitMapping.getDefaultValue() != null && !limitMapping.getDefaultValue().isBlank()) {
                        webService1.setResultLimit(Long.valueOf(limitMapping.getDefaultValue().trim()));
                    }
                    if (rd.visibleOn != null) webService1.setVisibleOn(rd.visibleOn);

                    List<WFSDatasetExport.Parameter> mapParameters = new ArrayList<>();
                    int position = 0;
                    for (Mapping mapping1 : rd.mappings) {
                        if (isWfsReservedParameter(mapping1.getVariable())) continue;
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
                        if(isPathParameter(templateSplit[0], mapping1.getVariable()))
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

        normalizeDraftParameters(item);
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
        metadata.setSoftwareRequirements(softwareSourceCode.getSoftwareRequirements());
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
