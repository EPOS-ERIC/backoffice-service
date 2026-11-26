package org.epos.backoffice.api.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.epos.eposdatamodel.Mapping;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MigrationDraftParameterTest {
    @Test
    void normalizesLegacyKeywordsForGateway() {
        assertEquals(List.of("geologia attivita", "rock soil", "abc-123:test_ok"),
                MigrationExecution.splitKeywords("geologia/attività, rock & soil, abc-123:test_ok, !!!"));
    }
    @Test
    void normalizesDateDefaultsWithoutLosingExistingOffsets() {
        assertEquals("2010-01-01T00:00:00Z", MigrationExecution.dateTimeDefault("2010-01-01T00:00:00"));
        assertEquals("2010-10-01T00:00:00Z", MigrationExecution.dateTimeDefault("2010-10-01"));
        assertEquals("2010-01-01T00:00:00+02:00", MigrationExecution.dateTimeDefault("2010-01-01T00:00:00+02:00"));
        assertThrows(IllegalArgumentException.class, () -> MigrationExecution.dateTimeDefault("invalid"));
    }
    @Test
    void convertsSupportedLegacyDateLayouts() {
        assertEquals("YYYY-MM-DD", MigrationExecution.dateTimeLayout("yyyy-MM-dd"));
        assertEquals("YYYY-MM-DDThh:mm:ssZ", MigrationExecution.dateTimeLayout("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        assertEquals("YYYY-MM-DDThh:mm:ssZ", MigrationExecution.dateTimeLayout("YYYY-MM-DDTHH:MM:SSZ"));
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
