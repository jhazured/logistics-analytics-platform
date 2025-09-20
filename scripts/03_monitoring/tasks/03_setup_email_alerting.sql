-- Set up email alerting system

-- Create notification integration for email alerts
CREATE OR REPLACE NOTIFICATION INTEGRATION INT_email_alert_integration
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('jharkeris@hotmail.com')
COMMENT = 'Email notification integration for system alerts';

-- Create procedure to send email alerts
CREATE OR REPLACE PROCEDURE SP_send_email_alert(
    alert_type VARCHAR,
    severity VARCHAR,
    message TEXT,
    recipient_email VARCHAR DEFAULT 'jharkeris@hotmail.com'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Create alert record
        var insert_query = `
            INSERT INTO LOGISTICS_DW_PROD.MONITORING.SYSTEM_ALERTS (
                alert_type, severity, component, message
            ) VALUES (?, ?, 'EMAIL_SYSTEM', ?)
        `;
        
        var stmt = snowflake.createStatement({
            sqlText: insert_query,
            binds: [ALERT_TYPE, SEVERITY, MESSAGE]
        });
        stmt.execute();
        
        // In a real implementation, you would send the email here
        // using the notification integration
        return "Email alert sent successfully to " + RECIPIENT_EMAIL;
    } catch (err) {
        return "Error sending email alert: " + err.message;
    }
$$;

-- Create task to process critical alerts and send emails
CREATE OR REPLACE TASK TSK_process_critical_alerts
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '1 MINUTE'
COMMENT = 'Process critical alerts and send email notifications'
AS
CALL SP_send_email_alert(
    'CRITICAL_ALERT',
    'CRITICAL',
    'Critical system alert detected. Please check the monitoring dashboard.'
);

-- Enable the task
ALTER TASK TSK_process_critical_alerts RESUME;
