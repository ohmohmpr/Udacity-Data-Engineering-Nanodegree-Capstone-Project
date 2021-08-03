class SqlQueries:
    demographics_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_demographics
        WHERE state_id IS NULL;
    """)

    port_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_i94port
        WHERE port_code IS NULL;
    """)

    addr_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_i94addr
        WHERE state_id IS NULL;
    """)

    visa_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_i94visa
        WHERE visa_id IS NULL;
    """)

    city_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_i94cit_res
        WHERE city_id IS NULL;
    """)

    mode_fact_table_check_null = ("""
        SELECT COUNT(*)
        FROM dim_i94mode
        WHERE Transport_Code IS NULL;
    """)
    
    immigration_fact_table_check_null =  ("""
        SELECT  COUNT(*) 
        from fact_immigrations
        WHERE cicid IS NULL;
    """)
    immigration_fact_table_insert = ("""
        INSERT INTO {} (
            {}
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
    # WHERE stg_i.i94mon={};
    demographics_table_insert = ("""
        INSERT INTO {} (
            {}          
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

    port_table_insert = ("""
        INSERT INTO {} (
            {}
        ) SELECT stg_p.port_code     AS port_code,
                 stg_p.port          AS port
        FROM staging_i94port as stg_p;
    """)

    addr_table_insert = ("""
        INSERT INTO {} (
            {}
        ) SELECT stg_a.abbreviation_code     AS abbreviation_code,
                stg_a.fullname_state         AS fullname_state
        FROM staging_i94addr as stg_a;
    """)

    visa_table_insert = ("""
        INSERT INTO {} (
            {}
        ) SELECT stg_v.visa_code     AS visa_code,
                 stg_v.reason      AS reason
        FROM staging_i94visa as stg_v;
    """)

    cit_res_table_insert = ("""
        INSERT INTO dim_i94cit_res (
            city_id, 
            city
        ) SELECT stg_c.city_code     AS city_id,
                stg_c.city          AS city
        FROM staging_i94cit_res as stg_c;
    """)

    mode_table_insert = ("""
        INSERT INTO dim_i94mode (
            transport_code, 
            transport_type
        ) SELECT stg_m.transport_code     AS transport_code,
                stg_m.transport_type      AS transport_type
        FROM staging_i94mode as stg_m;
    """)
    
