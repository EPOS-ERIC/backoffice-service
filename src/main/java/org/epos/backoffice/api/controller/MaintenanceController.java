package org.epos.backoffice.api.controller;

import commonapis.MaintenanceScheduler;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import org.epos.eposdatamodel.User;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping(value = "/admin/maintenance", produces = "application/json")
public class MaintenanceController {
    private final HttpServletRequest request;
    private final OrphanCleanupController orphanCleanup;

    public MaintenanceController(HttpServletRequest request, OrphanCleanupController orphanCleanup) {
        this.request = request;
        this.orphanCleanup = orphanCleanup;
    }

    @PostMapping("/start")
    public ResponseEntity<?> start() {
        if (!isAdmin()) return forbidden();
        MaintenanceScheduler.start(Duration.ofMinutes(10), orphanCleanup::runScheduledCleanup);
        return ResponseEntity.ok(status());
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stop() {
        if (!isAdmin()) return forbidden();
        MaintenanceScheduler.stop();
        return ResponseEntity.ok(status());
    }

    @PostMapping("/run")
    public ResponseEntity<?> runNow() {
        if (!isAdmin()) return forbidden();
        return ResponseEntity.ok(orphanCleanup.runScheduledCleanup());
    }

    @GetMapping("/status")
    public ResponseEntity<?> statusEndpoint() {
        if (!isAdmin()) return forbidden();
        return ResponseEntity.ok(status());
    }

    @PreDestroy
    public void shutdown() {
        MaintenanceScheduler.stop();
    }

    private Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("running", MaintenanceScheduler.isRunning());
        result.put("intervalSeconds", MaintenanceScheduler.getInterval().toSeconds());
        return result;
    }

    private boolean isAdmin() {
        if (request.getSession(false) == null) return false;
        User user = (User) request.getSession(false).getAttribute("user");
        return user != null && Boolean.TRUE.equals(user.getIsAdmin());
    }

    private ResponseEntity<?> forbidden() {
        return ResponseEntity.status(403).body(Map.of("error", "Admin role required"));
    }
}
