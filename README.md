# Snowflake

####
COPY INTO <..> FROM @%<table_name> #using tbl stg
               FROM @~/<file_name> #user stg, single file
               FROM @~/.     FILE_FORMAT = (..) #user stg, multiple files
               
####
#Using named internal stage, DESCRIBE THE FILE_FORMAT
