-- Data masking policies for PII protection
CREATE OR REPLACE MASKING POLICY customer_email_mask AS (val STRING) 
RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_STEWARD') THEN val
    ELSE REGEXP