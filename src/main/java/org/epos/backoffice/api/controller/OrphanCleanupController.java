package org.epos.backoffice.api.controller;

import dao.EposDataModelDAO;
import jakarta.servlet.http.HttpServletRequest;
import model.*;
import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Administrative report and cleanup for stale metadata registry entries. */
@RestController
@RequestMapping(value = "/admin/orphans", produces = "application/json")
public class OrphanCleanupController {
    private static final Logger LOG = LoggerFactory.getLogger(OrphanCleanupController.class);

    private static final List<Class<?>> METADATA_TYPES = List.of(
            Attribution.class, Address.class, Element.class,
            Identifier.class, Quantitativevalue.class, Spatial.class, Temporal.class,
            Category.class, CategoryScheme.class, Contactpoint.class, Dataproduct.class,
            Distribution.class, Equipment.class, Facility.class, Mapping.class,
            OutputMapping.class, Payload.class, Operation.class, Organization.class,
            Person.class, Softwareapplication.class, Softwaresourcecode.class,
            Webservice.class
    );

    private final HttpServletRequest request;

    public OrphanCleanupController(HttpServletRequest request) {
        this.request = request;
    }

    @GetMapping
    public ResponseEntity<?> report() {
        if (!isAdmin()) return ResponseEntity.status(403).body(Map.of("error", "Admin role required"));
        return ResponseEntity.ok(scan(false));
    }

    @DeleteMapping
    public ResponseEntity<?> cleanup(
            @RequestParam(name = "confirm", required = false) String confirmation) {
        if (!isAdmin()) return ResponseEntity.status(403).body(Map.of("error", "Admin role required"));
        if (!"ORPHANS".equals(confirmation)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Confirmation required", "confirm", "ORPHANS"));
        }
        return ResponseEntity.ok(scan(true));
    }

    /** Invoked by the explicit maintenance scheduler without an HTTP request. */
    public Map<String, Object> runScheduledCleanup() {
        return scan(true);
    }

    private boolean isAdmin() {
        if (request.getSession(false) == null) return false;
        User user = (User) request.getSession(false).getAttribute("user");
        return user != null && Boolean.TRUE.equals(user.getIsAdmin());
    }

    private Map<String, Object> scan(boolean execute) {
        EposDataModelDAO dao = EposDataModelDAO.getInstance();
        Set<String> liveMetaIds = new LinkedHashSet<>();
        for (Class<?> type : METADATA_TYPES) {
            for (Object entity : dao.getAllFromDB(type)) {
                String metaId = readMetaId(entity);
                if (metaId != null) liveMetaIds.add(metaId);
            }
        }

        List<Map<String, Object>> orphans = new ArrayList<>();
        for (Object rawEntry : dao.getAllFromDB(EdmEntityId.class)) {
            EdmEntityId registryEntry = (EdmEntityId) rawEntry;
            if (registryEntry.getMetaId() == null || liveMetaIds.contains(registryEntry.getMetaId())) continue;
            List<AuthorizationGroup> groups = dao.getFromDBByUsingMultipleKeys(
                    Map.of("meta.metaId", registryEntry.getMetaId()), AuthorizationGroup.class);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("metaId", registryEntry.getMetaId());
            item.put("tableName", registryEntry.getTableName());
            item.put("groupReferences", groups.size());
            if (execute) {
                boolean groupsDeleted = true;
                for (AuthorizationGroup group : groups) {
                    groupsDeleted &= Boolean.TRUE.equals(dao.deleteObject(group));
                }
                boolean registryDeleted = groupsDeleted
                        && Boolean.TRUE.equals(dao.deleteObject(registryEntry));
                item.put("deleted", registryDeleted);
                if (!registryDeleted) LOG.error("Could not delete orphan meta_id={}", registryEntry.getMetaId());
            }
            orphans.add(item);
        }
        dao.clearAllCaches();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", execute ? "DELETE" : "REPORT");
        result.put("liveMetaIds", liveMetaIds.size());
        result.put("orphans", orphans);
        result.put("orphanCount", orphans.size());
        return result;
    }

    private String readMetaId(Object entity) {
        try {
            return (String) entity.getClass().getMethod("getMetaId").invoke(entity);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Metadata entity has no getMetaId(): "
                    + entity.getClass().getName(), error);
        }
    }
}
