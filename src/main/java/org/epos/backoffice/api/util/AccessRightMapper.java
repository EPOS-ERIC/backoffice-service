package org.epos.backoffice.api.util;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AccessRightMapper {

    /** Accetta sia http che https, con o senza slash finale */
    private static final Pattern EU_ACCESS_RIGHT = Pattern.compile(
            "^https?://publications\\.europa\\.eu/resource/authority/access-right/([A-Za-z_]+)/?$");

    /** Valori raccomandati da DCAT-AP */
    private static final Set<String> KNOWN_CODES = Set.of("PUBLIC", "RESTRICTED", "NON_PUBLIC");

    private AccessRightMapper() {}

    /**
     * "https://publications.europa.eu/resource/authority/access-right/PUBLIC" -> "PUBLIC".
     * Se il valore non è un URI EU di access-right, restituisce il valore originale.
     */
    public static String toEuCode(String accessRight) {
        if (accessRight == null || accessRight.isBlank()) return accessRight;

        Matcher m = EU_ACCESS_RIGHT.matcher(accessRight.trim());
        if (m.matches()) {
            return m.group(1).toUpperCase(Locale.ROOT);
        }
        // Già un codice "nudo" (es. "public") -> normalizzo
        String upper = accessRight.trim().toUpperCase(Locale.ROOT);
        return KNOWN_CODES.contains(upper) ? upper : accessRight;
    }
}