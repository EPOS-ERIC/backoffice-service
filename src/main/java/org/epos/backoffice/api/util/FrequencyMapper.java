package org.epos.backoffice.api.util;

import java.util.Locale;
import java.util.Map;

public final class FrequencyMapper {

    private static final String CLD_FREQ_PREFIX = "http://purl.org/cld/freq/";
    private static final String EU_FREQ_PREFIX  =
            "http://publications.europa.eu/resource/authority/frequency/";

    /** Termini CLD (in minuscolo) -> codici della tabella EU Frequency */
    private static final Map<String, String> EU_CODE_BY_CLD_TERM = Map.ofEntries(
            Map.entry("triennial",         "TRIENNIAL"),
            Map.entry("biennial",          "BIENNIAL"),
            Map.entry("annual",            "ANNUAL"),
            Map.entry("semiannual",        "ANNUAL_2"),
            Map.entry("threetimesayear",   "ANNUAL_3"),
            Map.entry("quarterly",         "QUARTERLY"),
            Map.entry("bimonthly",         "BIMONTHLY"),
            Map.entry("monthly",           "MONTHLY"),
            Map.entry("semimonthly",       "MONTHLY_2"),
            Map.entry("threetimesamonth",  "MONTHLY_3"),
            Map.entry("biweekly",          "BIWEEKLY"),
            Map.entry("weekly",            "WEEKLY"),
            Map.entry("semiweekly",        "WEEKLY_2"),
            Map.entry("threetimesaweek",   "WEEKLY_3"),
            Map.entry("daily",             "DAILY"),
            Map.entry("continuous",        "CONT"),
            Map.entry("irregular",         "IRREG")
    );

    private FrequencyMapper() {}

    /**
     * Converte un URI di frequenza (CLD o EU) nel codice EU, es.
     * "http://purl.org/cld/freq/irregular" -> "IRREG".
     * Se non è mappabile restituisce il valore originale.
     */
    public static String toEuCode(String frequency) {
        if (frequency == null || frequency.isBlank()) return frequency;

        // URI EU: basta prendere l'ultimo segmento
        if (frequency.startsWith(EU_FREQ_PREFIX)) {
            return frequency.substring(EU_FREQ_PREFIX.length());
        }

        String term = frequency.startsWith(CLD_FREQ_PREFIX)
                ? frequency.substring(CLD_FREQ_PREFIX.length())
                : frequency.substring(frequency.lastIndexOf('/') + 1);

        return EU_CODE_BY_CLD_TERM.getOrDefault(term.toLowerCase(Locale.ROOT), frequency);
    }
}
