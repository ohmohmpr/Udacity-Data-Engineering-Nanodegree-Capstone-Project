

## Data Dictionary

### fact_immigrations
fact table was created from sas file.

| Field Name | Data Type | Data Format | Field Size | Description | Example |
| ---------- | --------- | ----------- | ---------- | ----------- | ------- |
| immigrations_id |bigint|             |            | ID that uniquely identify one record in the dataset|1|
| cicid      | Double    |             |            | ID that uniquely identify one record in the old dataset in each month | 6.0 |
| i94yr      | Double    |    YYYY     |       4    | Year                                              | 2016.0  |
| i94mon     | Double    |       M     |       1    | Numeric month                                     |    4.0  |
| i94cit     | Double    |     XXX     |       3    | Source country for immigration (Born country)     |  692.0  |
| I94RES     | Double    |     XXX     |       3    | Source country for immigration (Residence country)|  692.0  |
| I94PORT    | varchar   |     XXX     |       3    |Port addmitted through                             |    ATL  |
| ARRDATE    | DATE      |  yyyy-MM-dd |       8    |the Arrival Date in the USA.                       |2016-01-01|
| i94mode    | Double    |       X     |       1    |Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported) |  1  |
| i94addr    | String    |      XX     |       2    |State of arrival                                   |     AL  |
| depdate    | DATE      |  yyyy-MM-dd |       8    |the Departure Date from the USA.                   |2016-01-01|
| i94visa    | Double    |       X     |       1    |Visa codes collapsed into three categories:1 = Business 2 = Pleasure 3 = Student |  1  |
| dtadfile   | DATE      |  yyyy-MM-dd |       8    |Character Date Field - Date added to I-94 Files    |2016-01-01|
| entdepa    | String    |        X    |       1    |Arrival Flag - admitted or paroled into the U.S.   |    G   |
| entdepd    | String    |        X    |       1    |Departure Flag - Departed, lost I-94 or is deceased|    N   |
| entdepu    | String    |        X    |       1    |Update Flag - Either apprehended, overstayed, adjusted to perm residence | U|
| matflag    | String    |        X    |       1    |Match flag - Match of arrival and departure records.|      M  |
| biryear    | Double    |     XXXX    |       4    |year of birth                                       | 1988.0  |
| dtaddto    | DATE      |  yyyy-MM-dd |       8    |Character Date Field                               |2016-01-01|
| GENDER     | String    |        X    |       1    |Non-immigrant sex.                                 |      M  |
| INSNUM     | String    |     XXXX    |       4    |INS number                                         |   3181  |
| airline    | String    |       XX    |       2    |Airline used to arrive in U.S.                     |     OS  |
| admnum     | Double    |             |            |Admission Number                                   |6.66643185E8|
| fltno      | String    |       XX    |       2    |Flight number of Airline used to arrive in U.S.    |     93  |
| visatype   | String    |       XX    |       2    |Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|  B2 |

### dim_demographics
US demographics table showed about population for each state.

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| state_id            |  varchar  |    XX       |     2      | abbreviation state code|533657|
| total_population    |  bigint   |    number   |            | number of total population |260944 |
| male_population     |  bigint   |    number   |            | number of male population|272713|
| female_population   |  bigint   |    number   |            | number of female population|33463|
| number_of_veterans  |  bigint   |    number   |            | number of veterans population|27744|
| foreign_born        |  bigint   |    number   |            | number of foreign-born population|3705|
| american_indian_and_alaska_native |  bigint  |    number   |            | number of american indian and alaska native population|13355|
| asian               |  bigint   |    number   |            | number of asian population|175064|
| black_or_african_american         |  bigint  |    number   |            | number of black or african american population|29863|
| hispanic_or_latino  |  bigint   |    number   |            | number of hispanic or latino  population|343764|
| white               |  bigint   |    number   |            | number of white population|175064|

### dim_i94visa
Table showed visa reason.

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| visa_id             |  varchar  |     X       |     1      | abbreviation state code|2|
| reason              |  varchar  |             |            | reason to go to US |Student |


### dim_i94port
US demographics table showed about population for each state.

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| port_code           |  varchar  |    XXX      |     3      | abbreviation port code|EPM|
| port                |  varchar  |             |            | fullname of port and state |EASTPORT, ME  |


### dim_i94addr
US demographics table showed about population for each state.

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| state_id            |  varchar  |    XX       |     2      | abbreviation state code|PA|
| fullname_state      |  varchar  |             |            | fullname of state |PENNSYLVANIA |


### dim_i94cit_res
Table show each code for city

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| city_id             |  varchar  |    XXX      |     3      | city code   |  118    |
| city                |  varchar  |             |            | city        |  LATVIA |


### dim_i94mode
Table showed each code for type of transport

| Field Name          | Data Type | Data Format | Field Size | Description | Example |
| ----------          | --------- | ----------- | ---------- | ----------- | ------- |
| Transport_Code      |  varchar  |    X        |     1      | transport code|1|
| Transport_Type      |  varchar  |             |            | type of transport |Air |

