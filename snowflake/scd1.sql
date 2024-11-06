-- Logic to merge stage data from S3 via Snowpipe into the main table customer, example of SCD1.
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