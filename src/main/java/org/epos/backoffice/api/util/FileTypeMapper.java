package org.epos.backoffice.api.util;

import java.util.Locale;
import java.util.Map;

public final class FileTypeMapper {

    private static final String EU_FILE_TYPE_PREFIX =
            "http://publications.europa.eu/resource/authority/file-type/";

    private static final Map<String, String> MIME_BY_CODE = Map.ofEntries(
            Map.entry("JSON",        "application/json"),
            Map.entry("JSON_LD",     "application/ld+json"),
            Map.entry("GEOJSON",     "application/geo+json"),
            Map.entry("XML",         "application/xml"),
            Map.entry("CSV",         "text/csv"),
            Map.entry("TSV",         "text/tab-separated-values"),
            Map.entry("TXT",         "text/plain"),
            Map.entry("HTML",        "text/html"),
            Map.entry("PDF",         "application/pdf"),
            Map.entry("ZIP",         "application/zip"),
            Map.entry("GZIP",        "application/gzip"),
            Map.entry("TAR",         "application/x-tar"),
            Map.entry("XLS",         "application/vnd.ms-excel"),
            Map.entry("XLSX",        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            Map.entry("DOCX",        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            Map.entry("RDF_XML",     "application/rdf+xml"),
            Map.entry("RDF_TURTLE",  "text/turtle"),
            Map.entry("N_TRIPLES",   "application/n-triples"),
            Map.entry("GML",         "application/gml+xml"),
            Map.entry("KML",         "application/vnd.google-earth.kml+xml"),
            Map.entry("KMZ",         "application/vnd.google-earth.kmz"),
            Map.entry("NETCDF",      "application/x-netcdf"),
            Map.entry("HDF5",        "application/x-hdf5"),
            Map.entry("TIFF",        "image/tiff"),
            Map.entry("PNG",         "image/png"),
            Map.entry("JPEG",        "image/jpeg"),
            Map.entry("SHP",         "application/x-shapefile")
    );

    private FileTypeMapper() {}

    /** Restituisce il MIME type, oppure il valore originale se non è mappabile. */
    public static String toMimeType(String format) {
        if (format == null || format.isBlank()) return format;
        // Se è già un MIME type, lo lascio com'è
        if (format.contains("/") && !format.startsWith("http")) return format;

        String code = format.startsWith(EU_FILE_TYPE_PREFIX)
                ? format.substring(EU_FILE_TYPE_PREFIX.length())
                : format.substring(format.lastIndexOf('/') + 1);

        return MIME_BY_CODE.getOrDefault(code.toUpperCase(Locale.ROOT), format);
    }
}