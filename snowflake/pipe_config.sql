create or replace storage integration s3_init_streaming_bucket
    type = external_stage
    storage_provider = S3
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::533267399285:role/snowflake-s3-connection'
    storage_allowed_locations = ('s3://dw-snowflake-learning-sn/stream_data/');



desc integration s3_init_streaming_bucket;

CREATE OR REPLACE FILE FORMAT SCD_PROJECT.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

create or replace stage SCD_PROJECT.SCD2.customer_ext_stage
url = 's3://dw-snowflake-learning-sn/stream_data/'
storage_integration = s3_init_streaming_bucket
file_format = SCD_PROJECT.SCD2.CSV;


list @customer_ext_stage;


create or replace pipe customer_s3_pipe
auto_ingest = true
as
copy into customer_raw
from @SCD_PROJECT.SCD2.customer_ext_stage;

show pipes;
select count(*) from customer_raw;
