-- View to format and capture changes from Stream that is present on customer (final table) to use to update the customer_history table (SCD2 table)
create or replace view v_customer_changing_data as (
--INSERT/FALSE - NEW ROW
with insert_row as (
select 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME,
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    UPDATE_TIMESTAMP,
    'I' as DML_TYPE
FROM scd_project.scd2.customer_table_changes
WHERE METADATA$ACTION = 'INSERT'
AND METADATA$ISUPDATE = 'FALSE'
),
-- The entry that will be marked current in the SCD2 table.
update_row_insert as(
select 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME,
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    UPDATE_TIMESTAMP,
    'U' as DML_TYPE
FROM scd_project.scd2.customer_table_changes
WHERE METADATA$ACTION = 'INSERT'
AND METADATA$ISUPDATE = 'TRUE'
),
-- The entry that will be marked not-current in the SCD2 table.
update_row_delete as (
select 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME,
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    UPDATE_TIMESTAMP,
    'D' as DML_TYPE
FROM scd_project.scd2.customer_table_changes
WHERE METADATA$ACTION = 'DELETE'
AND METADATA$ISUPDATE = 'TRUE'
),
-- Combining all the changes that need to be made
joined_table_changes as (
select * from insert_row
union
select * from update_row_insert
union 
select * from update_row_delete
)
-- Adjusting columns we are going to need in the merge logic into the SCD2 table customer_history
select  
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME,
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    UPDATE_TIMESTAMP as start_time,
    lag(UPDATE_TIMESTAMP) over(partition by customer_id order by UPDATE_TIMESTAMP desc) as end_time_raw,
    case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
    case when end_time_raw is null then TRUE else FALSE end as is_current,
    DML_TYPE
from joined_table_changes);

-- Merging from the view we made into the customer_history SCD2 table.
merge into customer_history ch
using v_customer_changing_data ccd
    on ch.customer_id = ccd.customer_id
    and ch.start_time = ccd.start_time
when matched and ccd.dml_type = 'D' then update
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when not matched then insert
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,START_TIME,END_TIME,IS_CURRENT)
    values (ccd.customer_id, ccd.first_name, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY, ccd.STATE, ccd.COUNTRY, ccd.START_TIME, ccd.END_TIME, ccd.IS_CURRENT);