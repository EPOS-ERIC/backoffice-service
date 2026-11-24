package org.epos.backoffice.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Java Bean che rappresenta una risorsa di tipo "software-source-code" con i relativi metadati.
 * Le annotazioni Jackson mappano i nomi snake_case del JSON sui campi camelCase.
 */
public class SoftwareSourceCodeExport implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Es. "software-source-code" */
    @JsonProperty("resource_type")
    private String resourceType;

    @JsonProperty("metadata")
    private Metadata metadata;

    public SoftwareSourceCodeExport() {
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
        private String name;

        @JsonProperty("description")
        private String description;

        @JsonProperty("keywords")
        private List<String> keywords = new ArrayList<>();

        @JsonProperty("repository")
        private String repository;

        @JsonProperty("programming_language")
        private String programmingLanguage;

        @JsonProperty("license")
        private String license;

        @JsonProperty("software_requirements")
        private String softwareRequirements;

        @JsonProperty("runtime_platform")
        private String runtimePlatform;

        @JsonProperty("main_entity_of_page")
        private String mainEntityOfPage;

        @JsonProperty("version")
        private String version;

        @JsonProperty("creators")
        private List<String> creators = new ArrayList<>();

        @JsonProperty("identifiers")
        private List<Identifier> identifiers = new ArrayList<>();

        @JsonProperty("categories")
        private List<String> categories = new ArrayList<>();

        @JsonProperty("contact_points")
        private List<String> contactPoints = new ArrayList<>();

        public Metadata() {
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }

        public List<String> getKeywords() { return keywords; }
        public void setKeywords(List<String> keywords) { this.keywords = keywords; }

        public String getRepository() { return repository; }
        public void setRepository(String repository) { this.repository = repository; }

        public String getProgrammingLanguage() { return programmingLanguage; }
        public void setProgrammingLanguage(String programmingLanguage) { this.programmingLanguage = programmingLanguage; }

        public String getLicense() { return license; }
        public void setLicense(String license) { this.license = license; }

        public String getSoftwareRequirements() { return softwareRequirements; }
        public void setSoftwareRequirements(String softwareRequirements) { this.softwareRequirements = softwareRequirements; }

        public String getRuntimePlatform() { return runtimePlatform; }
        public void setRuntimePlatform(String runtimePlatform) { this.runtimePlatform = runtimePlatform; }

        public String getMainEntityOfPage() { return mainEntityOfPage; }
        public void setMainEntityOfPage(String mainEntityOfPage) { this.mainEntityOfPage = mainEntityOfPage; }

        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }

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
        private String resourceId;

        @JsonProperty("type")
        private String type;

        @JsonProperty("value")
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