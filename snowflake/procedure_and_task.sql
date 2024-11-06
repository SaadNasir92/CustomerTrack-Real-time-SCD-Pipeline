create or replace procedure pdr_pipeline_scd2_customer()
returns string
language sql
as
$$
BEGIN
    -- Update main table from staging table (coming from S3 via SnowPipe)
    merge into customer c 
    using customer_raw cr
       on  c.customer_id = cr.customer_id
    when matched and (c.first_name  <> cr.first_name  or
                     c.last_name   <> cr.last_name   or
                     c.email       <> cr.email       or
                     c.street      <> cr.street      or
                     c.city        <> cr.city        or
                     c.state       <> cr.state       or
                     c.country     <> cr.country)
    then update
    set c.customer_id = cr.customer_id
            ,c.first_name  = cr.first_name 
            ,c.last_name   = cr.last_name  
            ,c.email       = cr.email      
            ,c.street      = cr.street     
            ,c.city        = cr.city       
            ,c.state       = cr.state      
            ,c.country     = cr.country  
            ,update_timestamp = current_timestamp()
    when not matched then insert
        (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    values 
        (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
    
        
    -- Truncate staging table
    truncate table customer_raw;
    
    
    -- Update SCD2 History Table
    merge into customer_history ch
    using v_customer_changing_data ccd
        on ch.customer_id = ccd.customer_id
        and ch.start_time = ccd.start_time
    when matched and ccd.dml_type = 'D' then update
        set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
    when not matched then 
    insert
        (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,START_TIME,END_TIME,IS_CURRENT)
    values 
        (ccd.customer_id, ccd.first_name, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY, ccd.STATE, ccd.COUNTRY, ccd.START_TIME, ccd.END_TIME, ccd.IS_CURRENT);

    RETURN 'Customer table & Customer History table update completed successfully.';
    
END;
$$;

-- create the task to run the procedure.
create or replace task task_customer_refresh
warehouse = COMPUTE_WH
schedule = '1 minute'
as
    call pdr_pipeline_scd2_customer();

    