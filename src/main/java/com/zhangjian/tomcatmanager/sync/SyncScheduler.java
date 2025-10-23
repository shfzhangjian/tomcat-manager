package com.zhangjian.tomcatmanager.sync;

import com.zhangjian.tomcatmanager.DatabaseConnection;
import com.zhangjian.tomcatmanager.DatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SyncScheduler {

    private static final Logger log = LoggerFactory.getLogger(SyncScheduler.class);
    private final EamSyncService eamSyncService;
    private final DatabaseService databaseService;

    public SyncScheduler(EamSyncService eamSyncService, DatabaseService databaseService) {
        this.eamSyncService = eamSyncService;
        this.databaseService = databaseService;
    }

    /**
     * 定时任务，每小时执行一次，用于触发启用了同步功能的数据连接。
     * Cron expression: second, minute, hour, day of month, month, day(s) of week
     * "0 0 * * * ?" = every hour at minute 0.
     */
    @Scheduled(cron = "0 0 * * * ?") // Runs every hour
    public void triggerSyncs() {
        log.info("Checking for scheduled sync tasks...");
        List<DatabaseConnection> allConnections = databaseService.getAllConnections();

        for (DatabaseConnection connection : allConnections) {
            // 检查每个连接的同步开关
            if (connection.isSyncEnabled()) {
                log.info("Triggering sync for enabled connection: {} ({})", connection.getName(), connection.getId());
                eamSyncService.executeSync(connection.getId());
            } else {
                log.info("Skipping sync for disabled connection: {} ({})", connection.getName(), connection.getId());
            }
        }
    }
}

