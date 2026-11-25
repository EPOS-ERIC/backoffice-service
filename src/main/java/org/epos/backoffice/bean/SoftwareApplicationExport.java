package org.epos.backoffice.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Java Bean che rappresenta una risorsa di tipo "software-application" con i relativi metadati.
 * Le annotazioni Jackson mappano i nomi snake_case del JSON sui campi camelCase.
 */
public class SoftwareApplicationExport implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Es. "software-application" */
    @JsonProperty("resource_type")
    @SerializedName("resource_type")
    private String resourceType;

    @JsonProperty("metadata")
    @SerializedName("metadata")
    private Metadata metadata;

    public SoftwareApplicationExport() {
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

        @JsonProperty("name")
        @SerializedName("name")
        private String name;

        @JsonProperty("description")
        @SerializedName("description")
        private String description;

        @JsonProperty("keywords")
        @SerializedName("keywords")
        private List<String> keywords = new ArrayList<>();

        @JsonProperty("license")
        @SerializedName("license")
        private String license;

        @JsonProperty("version")
        @SerializedName("version")
        private String version;

        @JsonProperty("download_url")
        @SerializedName("download_url")
        private String downloadUrl;

        @JsonProperty("requirements")
        @SerializedName("requirements")
        private String requirements;

        @JsonProperty("operating_system")
        @SerializedName("operating_system")
        private String operatingSystem;

        @JsonProperty("main_entity_of_page")
        @SerializedName("main_entity_of_page")
        private String mainEntityOfPage;

        @JsonProperty("creators")
        @SerializedName("creators")
        private List<String> creators = new ArrayList<>();

        @JsonProperty("identifiers")
        @SerializedName("identifiers")
        private List<Identifier> identifiers = new ArrayList<>();

        @JsonProperty("categories")
        @SerializedName("categories")
        private List<String> categories = new ArrayList<>();

        @JsonProperty("contact_points")
        @SerializedName("contact_points")
        private List<String> contactPoints = new ArrayList<>();

        public Metadata() {
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public List<String> getKeywords() { return keywords; }
        public void setKeywords(List<String> keywords) { this.keywords = keywords; }

        public String getLicense() { return license; }
        public void setLicense(String license) { this.license = license; }

        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }

        public String getDownloadUrl() { return downloadUrl; }
        public void setDownloadUrl(String downloadUrl) { this.downloadUrl = downloadUrl; }

        public String getRequirements() { return requirements; }
        public void setRequirements(String requirements) { this.requirements = requirements; }

        public String getOperatingSystem() { return operatingSystem; }
        public void setOperatingSystem(String operatingSystem) { this.operatingSystem = operatingSystem; }

        public String getMainEntityOfPage() { return mainEntityOfPage; }
        public void setMainEntityOfPage(String mainEntityOfPage) { this.mainEntityOfPage = mainEntityOfPage; }

        public List<String> getCreators() { return creators; }
        public void setCreators(List<String> creators) { this.creators = creators; }

        public List<Identifier> getIdentifiers() { return identifiers; }
        public void setIdentifiers(List<Identifier> identifiers) { this.identifiers = identifiers; }

        public List<String> getCategories() { return categories; }
        public void setCategories(List<String> categories) { this.categories = categories; }

        public List<String> getContactPoints() { return contactPoints; }
        public void setContactPoints(List<String> contactPoints) { this.contactPoints = contactPoints; }
    }

    // ------------------------------------------------------------------
    // Identifier
    // ------------------------------------------------------------------
    public static class Identifier implements Serializable {

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

        public Identifier() {
        }

        public String getResourceId() { return resourceId; }
        public void setResourceId(String resourceId) { this.resourceId = resourceId; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }
}