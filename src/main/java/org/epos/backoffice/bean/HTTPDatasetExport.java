package org.epos.backoffice.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Java Bean che rappresenta una risorsa (es. "dataset") con i relativi metadati.
 * Le annotazioni Jackson mappano i nomi snake_case del JSON sui campi camelCase.
 */
public class HTTPDatasetExport implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("resource_type")
    @SerializedName("resource_type")
    private String resourceType;

    @JsonProperty("metadata")
    @SerializedName("metadata")
    private Metadata metadata;

    public HTTPDatasetExport() {
    }

    public String getResourceType() { return resourceType; }
    public void setResourceType(String resourceType) { this.resourceType = resourceType; }

    public Metadata getMetadata() { return metadata; }
    public void setMetadata(Metadata metadata) { this.metadata = metadata; }

    // ------------------------------------------------------------------
    // Metadata
    // ------------------------------------------------------------------
    public static class Metadata implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("title")
        @SerializedName("title")
        private String title;

        @JsonProperty("description")
        @SerializedName("description")
        private String description;

        @JsonProperty("keywords")
        @SerializedName("keywords")
        private List<String> keywords = new ArrayList<>();

        /** Es. "TRIENNIAL" */
        @JsonProperty("accrual_periodicity")
        @SerializedName("accrual_periodicity")
        private String accrualPeriodicity;

        @JsonProperty("quality_annotation")
        @SerializedName("quality_annotation")
        private String qualityAnnotation;

        /** Es. "NON_PUBLIC" */
        @JsonProperty("access_right")
        @SerializedName("access_right")
        private String accessRight;

        @JsonProperty("distributions")
        @SerializedName("distributions")
        private List<Distribution> distributions = new ArrayList<>();

        /** Geometria in formato WKT, es. "POINT (12.5 41.9)" */
        @JsonProperty("spatial_coverage")
        @SerializedName("spatial_coverage")
        private String spatialCoverage;

        @JsonProperty("temporal_coverage")
        @SerializedName("temporal_coverage")
        private TemporalCoverage temporalCoverage;

        @JsonProperty("geological_temporal_coverage")
        @SerializedName("geological_temporal_coverage")
        private GeologicalTemporalCoverage geologicalTemporalCoverage;

        @JsonProperty("persistent_identifiers")
        @SerializedName("persistent_identifiers")
        private List<PersistentIdentifier> persistentIdentifiers = new ArrayList<>();

        @JsonProperty("contact_points")
        @SerializedName("contact_points")
        private List<String> contactPoints = new ArrayList<>();

        @JsonProperty("providers")
        @SerializedName("providers")
        private List<String> providers = new ArrayList<>();

        @JsonProperty("categories")
        @SerializedName("categories")
        private List<String> categories = new ArrayList<>();

        @JsonProperty("created")
        @SerializedName("created")
        private String created;

        @JsonProperty("updated")
        @SerializedName("updated")
        private String updated;

        public Metadata() {
        }

        public String getTitle() { return title; }
        public void setTitle(String title) { this.title = title; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public List<String> getKeywords() { return keywords; }
        public void setKeywords(List<String> keywords) { this.keywords = keywords; }

        public String getAccrualPeriodicity() { return accrualPeriodicity; }
        public void setAccrualPeriodicity(String accrualPeriodicity) { this.accrualPeriodicity = accrualPeriodicity; }

        public String getQualityAnnotation() { return qualityAnnotation; }
        public void setQualityAnnotation(String qualityAnnotation) { this.qualityAnnotation = qualityAnnotation; }

        public String getAccessRight() { return accessRight; }
        public void setAccessRight(String accessRight) { this.accessRight = accessRight; }

        public List<Distribution> getDistributions() { return distributions; }
        public void setDistributions(List<Distribution> distributions) { this.distributions = distributions; }

        public String getSpatialCoverage() { return spatialCoverage; }
        public void setSpatialCoverage(String spatialCoverage) { this.spatialCoverage = spatialCoverage; }

        public TemporalCoverage getTemporalCoverage() { return temporalCoverage; }
        public void setTemporalCoverage(TemporalCoverage temporalCoverage) { this.temporalCoverage = temporalCoverage; }

        public GeologicalTemporalCoverage getGeologicalTemporalCoverage() { return geologicalTemporalCoverage; }
        public void setGeologicalTemporalCoverage(GeologicalTemporalCoverage geologicalTemporalCoverage) { this.geologicalTemporalCoverage = geologicalTemporalCoverage; }

        public List<PersistentIdentifier> getPersistentIdentifiers() { return persistentIdentifiers; }
        public void setPersistentIdentifiers(List<PersistentIdentifier> persistentIdentifiers) { this.persistentIdentifiers = persistentIdentifiers; }

        public List<String> getContactPoints() { return contactPoints; }
        public void setContactPoints(List<String> contactPoints) { this.contactPoints = contactPoints; }

        public List<String> getProviders() { return providers; }
        public void setProviders(List<String> providers) { this.providers = providers; }

        public List<String> getCategories() { return categories; }
        public void setCategories(List<String> categories) { this.categories = categories; }

        public String getCreated() { return created; }
        public void setCreated(String created) { this.created = created; }

        public String getUpdated() { return updated; }
        public void setUpdated(String updated) { this.updated = updated; }
    }

    // ------------------------------------------------------------------
    // Distribution
    // ------------------------------------------------------------------
    public static class Distribution implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("resource_id")
        @SerializedName("resource_id")
        private String resourceId;

        @JsonProperty("title")
        @SerializedName("title")
        private String title;

        @JsonProperty("description")
        @SerializedName("description")
        private String description;

        @JsonProperty("license")
        @SerializedName("license")
        private String license;

        /** Es. "DOWNLOADABLE_FILE", "WEB_SERVICE" */
        @JsonProperty("type")
        @SerializedName("type")
        private String type;

        @JsonProperty("byte_size")
        @SerializedName("byte_size")
        private Long byteSize;

        /** Valorizzato quando type = "DOWNLOADABLE_FILE" */
        @JsonProperty("downloadable_file")
        @SerializedName("downloadable_file")
        private DownloadableFile downloadableFile;

        /** Valorizzato quando type = "WEB_SERVICE" */
        @JsonProperty("web_service")
        @SerializedName("web_service")
        private WebService webService;

        public Distribution() {
        }

        public String getResourceId() { return resourceId; }
        public void setResourceId(String resourceId) { this.resourceId = resourceId; }

        public String getTitle() { return title; }
        public void setTitle(String title) { this.title = title; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public String getLicense() { return license; }
        public void setLicense(String license) { this.license = license; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Long getByteSize() { return byteSize; }
        public void setByteSize(Long byteSize) { this.byteSize = byteSize; }

        public DownloadableFile getDownloadableFile() { return downloadableFile; }
        public void setDownloadableFile(DownloadableFile downloadableFile) { this.downloadableFile = downloadableFile; }

        public WebService getWebService() { return webService; }
        public void setWebService(WebService webService) { this.webService = webService; }
    }

    // ------------------------------------------------------------------
    // WebService
    // ------------------------------------------------------------------
    public static class WebService implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("resource_id")
        @SerializedName("resource_id")
        private String resourceId;

        /** Es. ["GRAPH"] */
        @JsonProperty("visible_on")
        @SerializedName("visible_on")
        private List<String> visibleOn = new ArrayList<>();

        @JsonProperty("provider")
        @SerializedName("provider")
        private String provider;

        @JsonProperty("documentation_uri")
        @SerializedName("documentation_uri")
        private String documentationUri;

        @JsonProperty("contact_points")
        @SerializedName("contact_points")
        private List<String> contactPoints = new ArrayList<>();

        /** Es. "DYNAMIC", "HTTP" */
        @JsonProperty("type")
        @SerializedName("type")
        private String type;

        @JsonProperty("template_url")
        @SerializedName("template_url")
        private String templateUrl;

        /** Es. "GET" */
        @JsonProperty("method")
        @SerializedName("method")
        private String method;

        @JsonProperty("parameters")
        @SerializedName("parameters")
        private List<Parameter> parameters = new ArrayList<>();

        @JsonProperty("body_content_type")
        @SerializedName("body_content_type")
        private String bodyContentType;

        @JsonProperty("body_template")
        @SerializedName("body_template")
        private String bodyTemplate;

        @JsonProperty("body_required")
        @SerializedName("body_required")
        private Boolean bodyRequired;

        @JsonProperty("body_parameters")
        @SerializedName("body_parameters")
        private List<BodyParameter> bodyParameters = new ArrayList<>();

        @JsonProperty("format")
        @SerializedName("format")
        private String format;

        public WebService() {
        }

        public String getResourceId() { return resourceId; }
        public void setResourceId(String resourceId) { this.resourceId = resourceId; }

        public List<String> getVisibleOn() { return visibleOn; }
        public void setVisibleOn(List<String> visibleOn) { this.visibleOn = visibleOn; }

        public String getProvider() { return provider; }
        public void setProvider(String provider) { this.provider = provider; }

        public String getDocumentationUri() { return documentationUri; }
        public void setDocumentationUri(String documentationUri) { this.documentationUri = documentationUri; }

        public List<String> getContactPoints() { return contactPoints; }
        public void setContactPoints(List<String> contactPoints) { this.contactPoints = contactPoints; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getTemplateUrl() { return templateUrl; }
        public void setTemplateUrl(String templateUrl) { this.templateUrl = templateUrl; }

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public List<Parameter> getParameters() { return parameters; }
        public void setParameters(List<Parameter> parameters) { this.parameters = parameters; }

        public String getBodyContentType() { return bodyContentType; }
        public void setBodyContentType(String bodyContentType) { this.bodyContentType = bodyContentType; }

        public String getBodyTemplate() { return bodyTemplate; }
        public void setBodyTemplate(String bodyTemplate) { this.bodyTemplate = bodyTemplate; }

        public Boolean getBodyRequired() { return bodyRequired; }
        public void setBodyRequired(Boolean bodyRequired) { this.bodyRequired = bodyRequired; }

        public List<BodyParameter> getBodyParameters() { return bodyParameters; }
        public void setBodyParameters(List<BodyParameter> bodyParameters) { this.bodyParameters = bodyParameters; }

        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
    }

    // ------------------------------------------------------------------
    // BodyParameter (campi comuni a tutti i parametri)
    // ------------------------------------------------------------------
    public static class BodyParameter implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("resource_id")
        @SerializedName("resource_id")
        private String resourceId;

        /** Es. "STRING" */
        @JsonProperty("kind")
        @SerializedName("kind")
        private String kind;

        @JsonProperty("position")
        @SerializedName("position")
        private Integer position;

        @JsonProperty("name")
        @SerializedName("name")
        private String name;

        @JsonProperty("label")
        @SerializedName("label")
        private String label;

        @JsonProperty("description")
        @SerializedName("description")
        private String description;

        @JsonProperty("required")
        @SerializedName("required")
        private Boolean required;

        @JsonProperty("property")
        @SerializedName("property")
        private String property;

        /** Es. "FORMAT" */
        @JsonProperty("role")
        @SerializedName("role")
        private String role;

        @JsonProperty("read_only")
        @SerializedName("read_only")
        private Boolean readOnly;

        /** "default" è una parola riservata in Java */
        @JsonProperty("default")
        @SerializedName("default")
        private List<String> defaultValues = new ArrayList<>();

        /** "enum" è una parola riservata in Java */
        @JsonProperty("enum")
        @SerializedName("enum")
        private List<String> enumValues = new ArrayList<>();

        @JsonProperty("pattern")
        @SerializedName("pattern")
        private String pattern;

        public BodyParameter() {
        }

        public String getResourceId() { return resourceId; }
        public void setResourceId(String resourceId) { this.resourceId = resourceId; }

        public String getKind() { return kind; }
        public void setKind(String kind) { this.kind = kind; }

        public Integer getPosition() { return position; }
        public void setPosition(Integer position) { this.position = position; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public Boolean getRequired() { return required; }
        public void setRequired(Boolean required) { this.required = required; }

        public String getProperty() { return property; }
        public void setProperty(String property) { this.property = property; }

        public String getRole() { return role; }
        public void setRole(String role) { this.role = role; }

        public Boolean getReadOnly() { return readOnly; }
        public void setReadOnly(Boolean readOnly) { this.readOnly = readOnly; }

        public List<String> getDefaultValues() { return defaultValues; }
        public void setDefaultValues(List<String> defaultValues) { this.defaultValues = defaultValues; }

        public List<String> getEnumValues() { return enumValues; }
        public void setEnumValues(List<String> enumValues) { this.enumValues = enumValues; }

        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
    }

    // ------------------------------------------------------------------
    // Parameter (parametro dell'URL: aggiunge location, explode, multiple_values)
    // ------------------------------------------------------------------
    public static class Parameter extends BodyParameter {

        private static final long serialVersionUID = 1L;

        /** Es. "PATH", "QUERY" */
        @JsonProperty("location")
        @SerializedName("location")
        private String location;

        @JsonProperty("explode")
        @SerializedName("explode")
        private Boolean explode;

        @JsonProperty("multiple_values")
        @SerializedName("multiple_values")
        private Boolean multipleValues;

        public Parameter() {
        }

        public String getLocation() { return location; }
        public void setLocation(String location) { this.location = location; }

        public Boolean getExplode() { return explode; }
        public void setExplode(Boolean explode) { this.explode = explode; }

        public Boolean getMultipleValues() { return multipleValues; }
        public void setMultipleValues(Boolean multipleValues) { this.multipleValues = multipleValues; }
    }

    // ------------------------------------------------------------------
    // DownloadableFile
    // ------------------------------------------------------------------
    public static class DownloadableFile implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("url")
        @SerializedName("url")
        private String url;

        @JsonProperty("format")
        @SerializedName("format")
        private String format;

        public DownloadableFile() {
        }

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
    }

    // ------------------------------------------------------------------
    // TemporalCoverage
    // ------------------------------------------------------------------
    public static class TemporalCoverage implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("start")
        @SerializedName("start")
        private String start;

        @JsonProperty("end")
        @SerializedName("end")
        private String end;

        public TemporalCoverage() {
        }

        public String getStart() { return start; }
        public void setStart(String start) { this.start = start; }

        public String getEnd() { return end; }
        public void setEnd(String end) { this.end = end; }
    }

    // ------------------------------------------------------------------
    // GeologicalTemporalCoverage
    // ------------------------------------------------------------------
    public static class GeologicalTemporalCoverage implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Milioni di anni fa (inizio) */
        @JsonProperty("start_mya")
        @SerializedName("start_mya")
        private Double startMya;

        /** Milioni di anni fa (fine) */
        @JsonProperty("end_mya")
        @SerializedName("end_mya")
        private Double endMya;

        public GeologicalTemporalCoverage() {
        }

        public Double getStartMya() { return startMya; }
        public void setStartMya(Double startMya) { this.startMya = startMya; }

        public Double getEndMya() { return endMya; }
        public void setEndMya(Double endMya) { this.endMya = endMya; }
    }

    // ------------------------------------------------------------------
    // PersistentIdentifier
    // ------------------------------------------------------------------
    public static class PersistentIdentifier implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("resource_id")
        @SerializedName("resource_id")
        private String resourceId;

        @JsonProperty("type")
        @SerializedName("type")
        private String type;

        @JsonProperty("value")
        @SerializedName("value")
        private String value;

        public PersistentIdentifier() {
        }

        public String getResourceId() { return resourceId; }
        public void setResourceId(String resourceId) { this.resourceId = resourceId; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }
}