package org.epos.backoffice.api.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.epos.eposdatamodel.Mapping;
import org.epos.eposdatamodel.Category;
import org.epos.eposdatamodel.LinkedEntity;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MigrationDraftParameterTest {
    @Test
    void buildsCapabilitiesUrlFromLegacyMapTemplate() {
        String legacy = "https://geo.example.org/ows?service=WMS&version=1.3.0&request=GetMap{&layers,width}&bbox={minlat, maxlat}&token=public";
        assertEquals("https://geo.example.org/ows?version=1.3.0&token=public&service=WMS&request=GetCapabilities",
                MigrationExecution.normalizeCapabilitiesUrl(legacy, "WMS"));
        assertEquals("https://geo.example.org/tiles?service=WMTS&request=GetCapabilities",
                MigrationExecution.normalizeCapabilitiesUrl("https://geo.example.org/tiles{?layer,format}", "WMTS"));
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.normalizeCapabilitiesUrl("not-a-url", "WMS"));
    }

    @Test
    void normalizesCapabilitiesInExportAndRecordsChange() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"title":"Map","web_service":{
                  "type":"WMS",
                  "template_url":"https://geo.example.org/ows?service=WMS&request=GetMap{&layers}&bbox={minlat,maxlat}",
                  "capabilities_url":"https://geo.example.org/ows?service=WMS&request=GetMap{&layers}&bbox={minlat,maxlat}",
                  "parameters":[]}}]}}
                """).getAsJsonObject();
        MigrationExecution.beginMigrationDebug();
        try {
            MigrationExecution.normalizeDraftParameters(payload);
            JsonObject service = payload.getAsJsonObject("metadata").getAsJsonArray("distributions").get(0)
                    .getAsJsonObject().getAsJsonObject("web_service");
            assertEquals("https://geo.example.org/ows?service=WMS&request=GetCapabilities", service.get("capabilities_url").getAsString());
            var events = MigrationExecution.endMigrationDebug();
            assertEquals(true, java.util.stream.StreamSupport.stream(events.spliterator(), false)
                    .anyMatch(event -> event.getAsJsonObject().get("action").getAsString().equals("capabilities_url_normalized")));
        } finally {
            MigrationExecution.endMigrationDebug();
        }
    }
    @Test
    void recordsCategoryAndSchemeIdentitiesIncludingUnresolvedReferences() {
        MigrationExecution.beginMigrationDebug();
        try {
            LinkedEntity ref = new LinkedEntity().instanceId("category-id");
            Category category = new Category();
            category.setName("Same name");
            category.setInScheme(new LinkedEntity().instanceId("scheme-id"));
            MigrationExecution.recordSourceCategory(ref, category);
            MigrationExecution.recordSourceCategory(new LinkedEntity().instanceId("missing"), null);
            var events = MigrationExecution.endMigrationDebug();
            assertEquals("category-id", events.get(0).getAsJsonObject().get("source_id").getAsString());
            assertEquals("scheme-id", events.get(0).getAsJsonObject().get("scheme_id").getAsString());
            assertEquals("missing", events.get(1).getAsJsonObject().get("source_id").getAsString());
            assertEquals(false, events.get(1).getAsJsonObject().has("scheme_id"));
        } finally {
            MigrationExecution.endMigrationDebug();
        }
    }
    @Test
    void recordsFallbackAndRetainsIncompleteWmsDistributionForReview() {
        MigrationExecution.beginMigrationDebug();
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[
                  {"title":"bad WMS","web_service":{"type":"WMS","template_url":"https://example.org/?q=prefix{variable}"}},
                  {"title":"file","downloadable_file":{"url":"https://example.org/file"}}
                ]}}
                """).getAsJsonObject();
        try {
            assertEquals("Name", MigrationExecution.descriptionOrName("", "Name"));
            MigrationExecution.normalizeDraftParameters(payload);
            assertEquals(2, payload.getAsJsonObject("metadata").getAsJsonArray("distributions").size());
            JsonObject retained = payload.getAsJsonObject("metadata").getAsJsonArray("distributions").get(0)
                    .getAsJsonObject().getAsJsonObject("web_service");
            assertEquals("HTTP", retained.get("type").getAsString());
            assertEquals("WMS", retained.getAsJsonObject("_review_original_service").get("type").getAsString());
            var events = MigrationExecution.endMigrationDebug();
            assertEquals("description_fallback", events.get(0).getAsJsonObject().get("action").getAsString());
            assertEquals("distribution_retained_for_review", events.get(2).getAsJsonObject().get("action").getAsString());
        } finally {
            MigrationExecution.endMigrationDebug();
        }
    }
    @Test
    void removesCompositeQueryPlaceholderAndKeepsFixedParameters() {
        JsonObject service = JsonParser.parseString("""
                {"type":"HTTP","template_url":"https://example.org/api?fixed=1&bbox={minLon,minLat,maxLon,maxLat}&other=2","parameters":[]}
                """).getAsJsonObject();
        MigrationExecution.normalizeQueryPlaceholders(service);
        assertEquals("https://example.org/api?fixed=1&other=2", service.get("template_url").getAsString());
    }
    @Test
    void fillsMissingDescriptionFromName() {
        assertEquals("Name", MigrationExecution.descriptionOrName(" ", "Name"));
        assertEquals("Description", MigrationExecution.descriptionOrName("Description", "Name"));
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.descriptionOrName("", ""));
    }
    @Test
    void preservesFixedQueryBeforeRfc6570Continuation() {
        JsonObject service = JsonParser.parseString("""
                {"type":"HTTP","template_url":"https://example.org/api?pt=MODEL{&start, end}",
                 "parameters":[{"name":"start","location":"QUERY"},{"name":"end","location":"QUERY"}]}
                """).getAsJsonObject();
        MigrationExecution.normalizeQueryPlaceholders(service);
        assertEquals("https://example.org/api?pt=MODEL", service.get("template_url").getAsString());
        assertEquals("QUERY", service.getAsJsonArray("parameters").get(0).getAsJsonObject().get("location").getAsString());
        JsonObject undefined = JsonParser.parseString("""
                {"type":"HTTP","template_url":"https://example.org/?pt=MODEL{&missing}","parameters":[]}
                """).getAsJsonObject();
        MigrationExecution.normalizeQueryPlaceholders(undefined);
        assertEquals("https://example.org/?pt=MODEL", undefined.get("template_url").getAsString());
    }
    @Test
    void fixesLegacyPathDatesAndBlankEnumValues() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"path","kind":"STRING","location":"PATH","required":false,"enum":["","valid"]},
                  {"name":"start","kind":"STRING","pattern":"YYYYMMDD","default":["19821001"]},
                  {"name":"publication_date","kind":"STRING","pattern":"YYYY-MM-DDThh:mm:ssZ","default":["2022-03-01T10:00:00Z"]}
                ]}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(payload);
        var parameters = payload.getAsJsonObject("metadata").getAsJsonArray("distributions").get(0)
                .getAsJsonObject().getAsJsonObject("web_service").getAsJsonArray("parameters");
        assertEquals(true, parameters.get(0).getAsJsonObject().get("required").getAsBoolean());
        assertEquals(1, parameters.get(0).getAsJsonObject().getAsJsonArray("enum").size());
        for (int i = 1; i < 3; i++) {
            JsonObject parameter = parameters.get(i).getAsJsonObject();
            assertEquals(true, java.util.regex.Pattern.compile(parameter.get("pattern").getAsString())
                    .matcher(parameter.getAsJsonArray("default").get(0).getAsString()).find());
        }
    }
    @Test
    void movesQueryPlaceholdersToQueryParameters() {
        JsonObject service = JsonParser.parseString("""
                {"type":"HTTP","template_url":"https://example.org/api?fixed=1&feature_layer={name}",
                 "parameters":[{"name":"name","location":"PATH"}]}
                """).getAsJsonObject();
        MigrationExecution.normalizeQueryPlaceholders(service);
        assertEquals("https://example.org/api?fixed=1", service.get("template_url").getAsString());
        assertEquals("feature_layer", service.getAsJsonArray("parameters").get(0).getAsJsonObject().get("name").getAsString());
        assertEquals("QUERY", service.getAsJsonArray("parameters").get(0).getAsJsonObject().get("location").getAsString());
    }
    @Test
    void addsMissingDefaultsToAllowedValues() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"format","kind":"STRING","default":["script"],"enum":["json","csv"]},
                  {"name":"west","role":"MIN_LON","multiple_values":false,"default":["2"],"enum":["1"]}
                ]}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(payload);
        JsonObject expected = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"format","kind":"STRING","default":["script"],"enum":[{"value":"json"},{"value":"csv"},{"value":"script"}]},
                  {"name":"west","role":"MIN_LON","kind":"FLOAT","multiple_values":false,"default":[2],"enum":[{"value":1},{"value":2}]}
                ]}}]}}
                """).getAsJsonObject();
        assertEquals(expected, payload);
    }
    @Test
    void normalizesLegacyKeywordsForGateway() {
        assertEquals(List.of("geologia attivita", "rock soil", "abc-123:test_ok"),
                MigrationExecution.splitKeywords("geologia/attività, rock & soil, abc-123:test_ok, !!!"));
    }
    @Test
    void normalizesDateDefaultsWithoutLosingExistingOffsets() {
        assertEquals("2010-01-01T00:00:00Z", MigrationExecution.dateTimeDefault("2010-01-01T00:00:00"));
        assertEquals("2010-10-01T00:00:00Z", MigrationExecution.dateTimeDefault("2010-10-01"));
        assertEquals("1982-10-01T00:00:00Z", MigrationExecution.dateTimeDefault("19821001"));
        assertEquals("2010-01-01T00:00:00Z", MigrationExecution.dateTimeDefault("2010"));
        assertEquals("2010-01-01T00:00:00+02:00", MigrationExecution.dateTimeDefault("2010-01-01T00:00:00+02:00"));
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.dateTimeDefault("invalid"));
    }
    @Test
    void convertsSupportedLegacyDateLayouts() {
        assertEquals("YYYY-MM-DD", MigrationExecution.dateTimeLayout("yyyy-MM-dd"));
        assertEquals("YYYY-MM-DDThh:mm:ssZ", MigrationExecution.dateTimeLayout("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        assertEquals("YYYY-MM-DDThh:mm:ssZ", MigrationExecution.dateTimeLayout("YYYY-MM-DDTHH:MM:SSZ"));
        assertEquals("YYYY-MM-DDThh:mm:ss.SSSZ", MigrationExecution.dateTimeLayout("YYYY-MM-DDThh:mm:ss.SSSZ"));
        assertEquals("YYYYMMDD", MigrationExecution.dateTimeLayout("YYYYMMDD"));
        assertEquals("YYYY", MigrationExecution.dateTimeLayout("YYYY"));
        assertEquals("YYYY-MM-DD hh:mm", MigrationExecution.dateTimeLayout("YYYY-MM-DD HH:mm"));
        assertEquals("YYYY-MM-DDThh:mm:ss", MigrationExecution.dateTimeLayout("YYYY-MM-DDTHH:MM:SS"));
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.dateTimeLayout("yyyy-MM-dd'T'HH:mm:ssXXX"));
    }
    @Test
    void selectsWfsChoicesWithoutInventingDefaults() {
        Mapping mapping = new Mapping();
        mapping.setParamValue(List.of("layer-a", "layer-b", "layer-a"));
        mapping.setDefaultValue("layer-b");
        List<String> choices = MigrationExecution.mappingChoices(mapping, "feature_types");
        assertEquals(List.of("layer-a", "layer-b"), choices);
        assertEquals("layer-b", MigrationExecution.mappingDefault(mapping, choices, "feature_types"));
        mapping.setDefaultValue(null);
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.mappingDefault(mapping, choices, "feature_types"));
    }
    @Test
    void translatesLegacyParameterPropertiesAndEnumValues() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"kind":"STRING","property":"source","role":"FORMAT","read_only":true,"default":["json"],"enum":["json","xml"]}
                ]}},{"downloadable_file":{"url":"https://example.org"}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(payload);
        JsonObject expected = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"kind":"STRING","role":"FORMAT","read_only":true,"default":["json"],"enum":[{"value":"json"},{"value":"xml"}]}
                ]}},{"downloadable_file":{"url":"https://example.org"}}]}}
                """).getAsJsonObject();
        assertEquals(expected, payload);
    }

    @Test
    void keepsEditableFormatParameterWithoutGatewayFormatRole() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"format","kind":"STRING","role":"FORMAT","read_only":false,"multiple_values":false,"default":["json"]}
                ]}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(payload);
        JsonObject parameter = payload.getAsJsonObject("metadata").getAsJsonArray("distributions").get(0)
                .getAsJsonObject().getAsJsonObject("web_service").getAsJsonArray("parameters").get(0).getAsJsonObject();
        assertEquals(false, parameter.has("role"));
        assertEquals(false, parameter.get("read_only").getAsBoolean());
        assertEquals("json", parameter.getAsJsonArray("default").get(0).getAsString());
    }

    @Test
    void usesGatewayTypesForCoordinateAndDateRoles() {
        JsonObject payload = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"west","kind":"STRING","role":"MIN_LON","multiple_values":false,"default":["12.5"],"enum":["12.5"]},
                  {"name":"start","kind":"STRING","role":"START_DATE","multiple_values":false,"default":["2020-01-01T00:00:00Z"]}
                ]}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(payload);
        JsonObject expected = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"west","kind":"FLOAT","role":"MIN_LON","multiple_values":false,"default":[12.5],"enum":[{"value":12.5}]},
                  {"name":"start","kind":"DATETIME","role":"START_DATE","default":"2020-01-01T00:00:00Z"}
                ]}}]}}
                """).getAsJsonObject();
        assertEquals(expected, payload);
    }

    @Test
    void validatesDynamicCoordinateConstraints() {
        JsonObject valid = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"lon","kind":"STRING","role":"LONGITUDE","required":true,"read_only":false,"multiple_values":false,"explode":false}
                ]}}]}}
                """).getAsJsonObject();
        MigrationExecution.normalizeDraftParameters(valid);
        assertEquals("FLOAT", valid.getAsJsonObject("metadata").getAsJsonArray("distributions").get(0)
                .getAsJsonObject().getAsJsonObject("web_service").getAsJsonArray("parameters").get(0)
                .getAsJsonObject().get("kind").getAsString());
        JsonObject invalid = JsonParser.parseString("""
                {"metadata":{"distributions":[{"web_service":{"parameters":[
                  {"name":"lon","role":"LONGITUDE","required":false,"read_only":false,"multiple_values":false,"explode":false}
                ]}}]}}
                """).getAsJsonObject();
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.normalizeDraftParameters(invalid));
    }
}
