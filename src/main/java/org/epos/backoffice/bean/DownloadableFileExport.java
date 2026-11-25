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
public class DownloadableFileExport implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("resource_type")
    @SerializedName("resource_type")
    private String resourceType;

    @JsonProperty("metadata")
    @SerializedName("metadata")
    private Metadata metadata;

    public DownloadableFileExport() {
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

        /** Es. "DOWNLOADABLE_FILE" */
        @JsonProperty("type")
        @SerializedName("type")
        private String type;

        @JsonProperty("byte_size")
        @SerializedName("byte_size")
        private Long byteSize;

        @JsonProperty("downloadable_file")
        @SerializedName("downloadable_file")
        private DownloadableFile downloadableFile;

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