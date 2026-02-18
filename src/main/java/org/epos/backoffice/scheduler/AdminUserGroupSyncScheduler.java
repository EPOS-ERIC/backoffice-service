package org.epos.backoffice.scheduler;

import dao.EposDataModelDAO;
import jakarta.annotation.PostConstruct;
import model.MetadataGroup;
import model.MetadataGroupUser;
import model.MetadataUser;
import model.RequestStatusType;
import model.RoleType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Scheduler that synchronizes admin users to all groups.
 * Runs on startup and then every minute, adding admin users to any groups they're not already members of.
 */
@Component
public class AdminUserGroupSyncScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(AdminUserGroupSyncScheduler.class);

    /**
     * Run sync immediately on application startup.
     */
    @PostConstruct
    public void onStartup() {
        LOG.info("[AdminUserGroupSyncScheduler] Running initial sync on startup...");
        try {
            int syncedCount = performSync();
            LOG.info("[AdminUserGroupSyncScheduler] Initial sync completed. Added {} new group memberships.", syncedCount);
        } catch (Exception e) {
            LOG.error("[AdminUserGroupSyncScheduler] Error during initial sync: {}", e.getMessage(), e);
        }
    }

    /**
     * Syncs all admin users to all groups every minute.
     * Admin users are added with role=ADMIN and status=ACCEPTED.
     * Users already in a group are skipped (no duplicates).
     */
    @Scheduled(cron = "0 * * * * *")  // Every minute at second 0
    public void syncAdminUsersToAllGroups() {
        LOG.debug("[AdminUserGroupSyncScheduler] Starting scheduled admin user sync...");
        
        try {
            int syncedCount = performSync();
            if (syncedCount > 0) {
                LOG.info("[AdminUserGroupSyncScheduler] Sync completed. Added {} new group memberships.", syncedCount);
            } else {
                LOG.debug("[AdminUserGroupSyncScheduler] Sync completed. No new memberships needed.");
            }
        } catch (Exception e) {
            LOG.error("[AdminUserGroupSyncScheduler] Error during sync: {}", e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private int performSync() {
        EposDataModelDAO dbAccess = EposDataModelDAO.getInstance();
        int syncCount = 0;

        // Get all admin users
        List<MetadataUser> allUsers = (List<MetadataUser>) dbAccess.getAllFromDB(MetadataUser.class);
        List<MetadataUser> adminUsers = allUsers.stream()
                .filter(u -> "true".equalsIgnoreCase(u.getIsadmin()))
                .collect(Collectors.toList());

        if (adminUsers.isEmpty()) {
            LOG.debug("[AdminUserGroupSyncScheduler] No admin users found.");
            return 0;
        }

        // Get all groups
        List<MetadataGroup> allGroups = (List<MetadataGroup>) dbAccess.getAllFromDB(MetadataGroup.class);

        if (allGroups.isEmpty()) {
            LOG.debug("[AdminUserGroupSyncScheduler] No groups found.");
            return 0;
        }

        LOG.debug("[AdminUserGroupSyncScheduler] Found {} admin users and {} groups.", 
                  adminUsers.size(), allGroups.size());

        for (MetadataUser admin : adminUsers) {
            for (MetadataGroup group : allGroups) {
                // Check if user is already a member of this group
                Map<String, Object> filters = new HashMap<>();
                filters.put("group.id", group.getId());
                filters.put("authIdentifier.authIdentifier", admin.getAuthIdentifier());

                List<MetadataGroupUser> existing = (List<MetadataGroupUser>) dbAccess.getFromDBByUsingMultipleKeys(
                        filters, MetadataGroupUser.class);

                if (existing.isEmpty()) {
                    // Add admin to group
                    MetadataGroupUser membership = new MetadataGroupUser();
                    membership.setId(UUID.randomUUID().toString());
                    membership.setGroup(group);
                    membership.setAuthIdentifier(admin);
                    membership.setRole(RoleType.ADMIN.name());
                    membership.setRequestStatus(RequestStatusType.ACCEPTED.name());

                    boolean success = dbAccess.updateObject(membership);
                    if (success) {
                        syncCount++;
                        LOG.debug("[AdminUserGroupSyncScheduler] Added admin '{}' to group '{}'", 
                                  admin.getAuthIdentifier(), group.getName());
                    }
                }
            }
        }

        // Invalidate caches if any changes were made
        if (syncCount > 0) {
            dbAccess.invalidateAllCachesForClass("MetadataGroupUser");
        }

        return syncCount;
    }
}
