-- Resource Monitor Usage View
CREATE OR REPLACE VIEW ANALYTICS.view_resource_monitor_usage AS
SELECT 
    rm.name AS monitor_name,
    rm.credit_quota,
    rm.used_credits,
    rm.remaining_credits,
    ROUND((rm.used_credits / rm.credit_quota) * 100, 2) AS usage_percentage,
    rm.level AS trigger_level,
    rm.frequency,
    rm.start_time,
    rm.end_time,
    rm.suspend_at,
    rm.suspend_immediately_at,
    rm.notify_at
FROM TABLE(INFORMATION_SCHEMA.RESOURCE_MONITOR_USAGE()) rm
ORDER BY usage_percentage DESC;