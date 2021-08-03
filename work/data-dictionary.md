

## Data Dictionary

### sas
Citizenship and Immigration
Adv of data dict is to force people who work with them to understand the data

| Field Name | Data Type | Data Format | Field Size | Description | Example |
| ---------- | --------- | ----------- | ---------- | ----------- | ------- |
| CICID      | Double    |             |            | ID that uniquely identify one record in the dataset (Citizenship and Immigration) | 6.0 |
| I94YR      | Double    |    YYYY     |       4    | Year                                              | 2016.0  |
| I94MON     | Double    |       M     |       1    |Numeric month                                      |    4.0  |
| I94CIT     | Double    |     XXX     |       3    |source city for immigration (Born country)         |  692.0  |
| I94RES     | Double    |     XXX     |       3    |source country for immigration (Residence country) |  692.0  |
| I94PORT    | String    |     XXX     |       3    |Port addmitted through                             |    ATL  |
| ARRDATE    | Double    |   XXXXX     |       5    |the Arrival Date in the USA. It is a SAS date numeric field that a permament format has not been applied.  Please apply whichever date format works for you.| 20573.0 |
| I94MODE    | Double    |       X     |       1    |Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)              |  1  |
| I94ADDR    | String    |      XX     |       2    |State of arrival     (458)                         |     AL  |
| DEPDATE    | Double    |   XXXXX     |       5    |the Departure Date from the USA. It is a SAS date numeric field that a permament format has not been applied.  Please apply whichever date format works for you.| 20691.0 |
| I94BIR     | Double    |  XX/XXX     |       2    |Age of Respondent in Years                         |   55.0  |
| I94VISA    | Double    |       X     |       1    |Visa codes collapsed into three categories:1 = Business 2 = Pleasure 3 = Student   |  1  |
| COUNT      | Double    |       X     |       1    |Used for summary statistics                        |    1.0  |
| DTADFILE   | String    | XXXXXXXX    |       8    |Character Date Field - Date added to I-94 Files - CIC does not use              |20160401|
| VISAPOST   | String    |      XXX    |       3    |Department of State where where Visa was issued - CIC does not use  (531)     |  PNM   |
| OCCUP      | String    |      XXX    |       3    |Occupation that will be performed in U.S. - CIC does not use    .<br> There are 122 classes. |  ELT   |
| ENTDEPA    | String    |        X    |       1    |Arrival Flag - admitted or paroled into the U.S. - CIC does not use.<br> There are 14 classes. <br>&nbsp; null <br>&nbsp; A<br>&nbsp; B<br> &nbsp; F<br> &nbsp; G<br> &nbsp; H<br> &nbsp; K<br> &nbsp; M<br> &nbsp; N<br> &nbsp; O<br> &nbsp; P<br> &nbsp; T<br> &nbsp; U<br> &nbsp; Z|    G   |
| ENTDEPD    | String    |        X    |       1    |Departure Flag - Departed, lost I-94 or is deceased - CIC does not use.<br> There are 13 classes. <br>&nbsp; null <br>&nbsp; D<br>&nbsp; I<br> &nbsp; J<br> &nbsp; K<br> &nbsp; L<br> &nbsp; M<br> &nbsp; N<br> &nbsp; O<br> &nbsp; Q<br> &nbsp; R<br> &nbsp; V<br> &nbsp; W |    N   |
| ENTDEPU    | String    |        X    |       1    |Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use . There are 3 classed  <br>&nbsp; null <br>&nbsp; U <br>&nbsp; Y | U|
| MATFLAG    | String    |        X    |       1    |Match flag - Match of arrival and departure records. There are 2 classed  <br>&nbsp; null <br>&nbsp; M|      M  |
| BIRYEAR    | Double    |     XXXX    |       4    |year of birth                                      | 1988.0  |
| DTADDTO    | String    | XXXXXXXX    |       8    |Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use |09302016|
| GENDER     | String    |        X    |       1    |Non-immigrant sex. There are 5 classed  <br>&nbsp; null <br>&nbsp; F<br>&nbsp; M<br> &nbsp; U<br> &nbsp; X |      M  |
| INSNUM     | String    |     XXXX    |       4    |INS number                     (infinite)          |   3181  |
| AIRLINE    | String    |       XX    |       2    |Airline used to arrive in U.S. (infinite)          |     OS  |
| ADMNUM     | Double    |             |            |Admission Number                                   |6.66643185E8|
| FLTNO      | String    |       XX    |       2    |Flight number of Airline used to arrive in U.S. (infinite)   |     93  |
| VISATYPE   | String    |       XX    |       2    |Class of admission legally admitting the non-immigrant to temporarily stay in U.S.<br> There are 17 classes. <br>&nbsp; B1 <br>&nbsp; B2<br>&nbsp; CP<br> &nbsp; CPL<br> &nbsp; E1<br> &nbsp; E2<br> &nbsp; F1<br> &nbsp; F2<br> &nbsp; GMB<br> &nbsp; GMT<br> &nbsp; I<br> &nbsp; I1<br> &nbsp; M1<br> &nbsp; M2<br> &nbsp; SBP<br> &nbsp; WB<br>&nbsp; WT|  B2 |

I94PORT(immigration table) --> (airport_code table)