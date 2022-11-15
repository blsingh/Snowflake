# Snowflake

####
COPY INTO <..> FROM @%<table_name> #using tbl stg
               FROM @~/<file_name> #user stg, single file
               FROM @~/.     FILE_FORMAT = (..) #user stg, multiple files
               
####
#Using named internal stage,  when you'll load the data from the named internal stage into the table using the COPY command. You do not provide a file format in the COPY command if the named internal stage already has an associated file format.


