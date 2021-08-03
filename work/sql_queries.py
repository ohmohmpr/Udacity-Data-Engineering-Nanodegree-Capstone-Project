import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN                 = config.get("IAM_ROLE","ARN")
DEMOGRAPHICS_DATA   = config.get("S3","DEMOGRAPHICS_DATA")
IMMIGRATIONS_DATA   = config.get("S3","IMMIGRATIONS_DATA")
ADDR_DATA           = config.get("S3","ADDR_DATA")
PORT_DATA           = config.get("S3","PORT_DATA")
VISA_DATA           = config.get("S3","VISA_DATA")
CITY_DATA           = config.get("S3","CITY_DATA")
MODE_DATA           = config.get("S3","MODE_DATA")

# DROP TABLES

staging_immigrations_table_drop = "DROP TABLE IF EXISTS staging_immigrations"
staging_demographics_table_drop = "DROP TABLE IF EXISTS staging_demographics"
staging_i94visa_table_drop = "DROP TABLE IF EXISTS staging_i94visa"
staging_i94port_table_drop = "DROP TABLE IF EXISTS staging_i94port"
staging_i94addr_table_drop = "DROP TABLE IF EXISTS staging_i94addr"
staging_i94cit_res_table_drop = "DROP TABLE IF EXISTS staging_i94cit_res"
staging_i94mode_table_drop = "DROP TABLE IF EXISTS staging_i94mode"

fact_immigrations_table_drop = "DROP TABLE IF EXISTS fact_immigrations"
dim_demographics_table_drop = "DROP TABLE IF EXISTS dim_demographics"
dim_i94visa_table_drop = "DROP TABLE IF EXISTS dim_i94visa"
dim_i94port_table_drop = "DROP TABLE IF EXISTS dim_i94port"
dim_i94addr_table_drop = "DROP TABLE IF EXISTS dim_i94addr"
dim_i94cit_res_table_drop = "DROP TABLE IF EXISTS dim_i94cit_res"
dim_i94mode_table_drop = "DROP TABLE IF EXISTS dim_i94mode"

staging_immigrations_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_immigrations (
        cicid          DOUBLE PRECISION,
        i94yr          DOUBLE PRECISION,
        i94mon         DOUBLE PRECISION,
        i94cit         DOUBLE PRECISION,
        i94res         DOUBLE PRECISION,
        i94port        varchar,
        arrdate        DATE,
        i94mode        DOUBLE PRECISION,
        i94addr        varchar,
        depdate        date,
        i94visa        DOUBLE PRECISION,
        dtadfile       date,
        entdepa        varchar, 
        entdepd        varchar,
        entdepu        varchar, 
        matflag        varchar,
        biryear        DOUBLE PRECISION,
        dtaddto        date,
        gender         varchar,
        airline        varchar, 
        admnum         DOUBLE PRECISION,
        fltno          varchar,
        visatype       varchar
    );
""")

staging_demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_demographics (
        state_code                          varchar, 
        total_population                    bigint,
        male_population                     bigint,
        female_population                   bigint,
        number_of_veterans                  bigint,
        foreign_born                        bigint,
        american_indian_and_alaska_native   bigint,
        asian                               bigint,
        black_or_african_american           bigint,
        hispanic_or_latino                  bigint,
        white                               bigint
    );
""")

staging_i94visa_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94visa (
        visa_code                     varchar, 
        reason                        varchar
    );
""")

staging_i94port_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94port (
        port_code                     varchar, 
        port                          varchar
    );
""")

staging_i94addr_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94addr (
        abbreviation_code            varchar, 
        fullname_state               varchar
    );
""")


staging_i94cit_res_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94cit_res (
        city_code                     varchar, 
        city                          varchar
    );
""")

staging_i94mode_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_i94mode (
        Transport_Code               varchar, 
        Transport_Type               varchar
    );
""")

## STAR SCHEMA

fact_immigrations_table_create= ("""
    CREATE TABLE IF NOT EXISTS fact_immigrations (
        immigrations_id bigint IDENTITY(0,1) PRIMARY KEY,
        cicid          DOUBLE PRECISION,
        i94yr          DOUBLE PRECISION,
        i94mon         DOUBLE PRECISION,
        i94cit         DOUBLE PRECISION,
        i94res         DOUBLE PRECISION,
        i94port        varchar,
        arrdate        DATE,
        i94mode        DOUBLE PRECISION,
        i94addr        varchar,
        depdate        date,
        i94visa        DOUBLE PRECISION,
        dtadfile       date,
        entdepa        varchar, 
        entdepd        varchar,
        entdepu        varchar, 
        matflag        varchar,
        biryear        DOUBLE PRECISION,
        dtaddto        date,
        gender         varchar,
        airline        varchar, 
        admnum         DOUBLE PRECISION,
        fltno          varchar,
        visatype       varchar
    )
""")

dim_demographics_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_demographics (
        state_id                            varchar, 
        total_population                    bigint,
        male_population                     bigint,
        female_population                   bigint,
        number_of_veterans                  bigint,
        foreign_born                        bigint,
        american_indian_and_alaska_native   bigint,
        asian                               bigint,
        black_or_african_american           bigint,
        hispanic_or_latino                  bigint,
        white                               bigint
    );
""")

dim_i94visa_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_i94visa (
        visa_id                     varchar, 
        reason                      varchar
    );
""")

dim_i94port_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_i94port (
        city_id                     varchar, 
        city                        varchar
    );
""")

dim_i94addr_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_i94addr (
        state_id            varchar, 
        fullname_state      varchar
    );
""")

dim_i94cit_res_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_i94cit_res (
        city_id                     varchar, 
        city                        varchar
    );
""")

dim_i94mode_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_i94mode (
        Transport_Code            varchar, 
        Transport_Type      varchar
    );
""")

# STAGING TABLES

staging_immigrations_copy = ("""
    copy staging_immigrations from {} 
    credentials 'aws_iam_role={}'
    format as parquet;
""").format(IMMIGRATIONS_DATA, ARN)

staging_demographics_copy = ("""
    copy staging_demographics from {}
    credentials  'aws_iam_role={}'
    format as parquet;
""").format(DEMOGRAPHICS_DATA, ARN)

staging_i94addr_copy = ("""
    copy staging_i94addr from {} 
    credentials 'aws_iam_role={}'
    format as parquet;
""").format(ADDR_DATA, ARN)

staging_i94port_copy = ("""
    copy staging_i94port from {}
    credentials  'aws_iam_role={}'
    format as parquet;
""").format(PORT_DATA, ARN)

staging_i94visa_copy = ("""
    copy staging_i94visa from {}
    credentials  'aws_iam_role={}'
    format as parquet;
""").format(VISA_DATA, ARN)

staging_i94cit_res_copy = ("""
    copy staging_i94cit_res from {}
    credentials  'aws_iam_role={}'
    format as parquet;
""").format(PORT_DATA, ARN)

staging_i94mode_copy = ("""
    copy staging_i94mode from {}
    credentials  'aws_iam_role={}'
    format as parquet;
""").format(VISA_DATA, ARN)

# FINAL TABLES

immigration_fact_table_insert = ("""
    INSERT INTO fact_immigrations (
        cicid,
        i94yr,
        i94mon,
        i94cit,
        i94res,
        i94port,
        arrdate,
        i94mode,
        i94addr,
        depdate,
        i94visa,
        dtadfile,
        entdepa, 
        entdepd,
        entdepu, 
        matflag,
        biryear,
        dtaddto,
        gender,
        airline, 
        admnum,
        fltno,
        visatype
    )
    SELECT DISTINCT 
        stg_i.cicid        AS  cicid,
        stg_i.i94yr        AS  i94yr,
        stg_i.i94mon       AS  i94mon,
        stg_i.i94cit       AS  i94cit,
        stg_i.i94res       AS  i94res,
        stg_i.i94port      AS  i94port,
        stg_i.arrdate      AS  arrdate,
        stg_i.i94mode      AS  i94mode,
        stg_i.i94addr      AS  i94addr,
        stg_i.depdate      AS  depdate,
        stg_i.i94visa      AS  i94visa,
        stg_i.dtadfile     AS  dtadfile,
        stg_i.entdepa      AS  entdepa, 
        stg_i.entdepd      AS  entdepd,
        stg_i.entdepu      AS  entdepu, 
        stg_i.matflag      AS  matflag,
        stg_i.biryear      AS  biryear,
        stg_i.dtaddto      AS  dtaddto,
        stg_i.gender       AS  gender,
        stg_i.airline      AS  airline, 
        stg_i.admnum       AS  admnum,
        stg_i.fltno        AS  fltno,
        stg_i.visatype     AS  visatype
    FROM staging_immigrations  AS stg_i 
    
""")
# JOIN songs ON songs.title = stg_e.song;
demographics_table_insert = ("""
    INSERT INTO dim_demographics (
        state_id, 
        total_population,
        male_population,
        female_population,
        number_of_veterans,
        foreign_born,
        american_indian_and_alaska_native,
        asian,
        black_or_african_american,
        hispanic_or_latino,
        white           
    ) 
    SELECT  stg_dem.state_code                              AS state_code,
            stg_dem.total_population                        AS total_population,
            stg_dem.male_population                         AS male_population,
            stg_dem.female_population                       AS female_population,
            stg_dem.number_of_veterans                      AS number_of_veterans,
            stg_dem.foreign_born                            AS foreign_born,
            stg_dem.american_indian_and_alaska_native       AS american_indian_and_alaska_native,
            stg_dem.asian                                   AS asian,
            stg_dem.black_or_african_american               AS black_or_african_american,
            stg_dem.hispanic_or_latino                      AS hispanic_or_latino,
            stg_dem.white                                   AS white
    FROM staging_demographics AS stg_dem
""")

i94visa_table_insert = ("""
    INSERT INTO dim_i94visa (
        visa_id, 
        reason
    ) SELECT stg_v.visa_code     AS visa_code,
             stg_v.reason      AS reason
    FROM staging_i94visa as stg_v;
""")

i94cit_res_table_insert = ("""
    INSERT INTO dim_i94cit_res (
        city_id, 
        city
    ) SELECT stg_c.city_code     AS city_id,
             stg_c.city          AS city
    FROM staging_i94cit_res as stg_c;
""")

i94mode_table_insert = ("""
    INSERT INTO dim_i94mode (
        transport_code, 
        transport_type
    ) SELECT stg_m.transport_code     AS transport_code,
             stg_m.transport_type      AS transport_type
    FROM staging_i94mode as stg_m;
""")

# QUERY LISTS

create_table_queries = [staging_immigrations_table_create, staging_demographics_table_create, 
                    staging_i94visa_table_create, staging_i94port_table_create, staging_i94addr_table_create, \
                    staging_i94cit_res_table_create, staging_i94mode_table_create, \
                    fact_immigrations_table_create, dim_demographics_table_create,  dim_i94visa_table_create, \
                    dim_i94port_table_create, dim_i94addr_table_create, \
                    dim_i94cit_res_table_create,  dim_i94mode_table_create
                    ]

# create_table_queries = [staging_immigrations_table_create, fact_immigrations_table_create]

drop_table_queries = [staging_immigrations_table_drop, staging_demographics_table_drop, staging_i94visa_table_drop, \
                    staging_i94port_table_drop, staging_i94addr_table_drop, \
                      staging_i94cit_res_table_drop, staging_i94mode_table_drop, \
                    fact_immigrations_table_drop, dim_demographics_table_drop, dim_i94visa_table_drop, \
                    dim_i94port_table_drop, dim_i94addr_table_drop, \
                      dim_i94cit_res_table_drop, dim_i94mode_table_drop]

# drop_table_queries = [staging_immigrations_table_drop, fact_immigrations_table_drop]

copy_table_queries = [staging_immigrations_copy, staging_demographics_copy, \
                      staging_i94cit_res_copy, staging_i94mode_copy, \
                    staging_i94addr_copy, staging_i94port_copy, staging_i94visa_copy] 

insert_table_queries = [immigration_fact_table_insert, demographics_table_insert, i94visa_table_insert,\
                       i94cit_res_table_insert, i94mode_table_insert]
