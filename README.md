# Snowflake

####
COPY INTO <..> FROM @%<table_name> # using tbl stg
               FROM @~/<file_name> 
               FROM @~/.     FILE_FORMAT = (..) #all 
