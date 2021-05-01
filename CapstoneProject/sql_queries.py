import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES

staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_immigration "
staging_airport_table_drop = "DROP TABLE IF EXISTS staging_airport"
staging_demography_table_drop = "DROP TABLE IF EXISTS staging_demography"
staging_landtemp_table_drop = "DROP TABLE IF EXISTS staging_landtemp"
dimStateDestination_table_drop = "DROP TABLE IF EXISTS dimStateDestination "
dimAirport_table_drop = "DROP TABLE IF EXISTS dimAirport"
dimTourist_table_drop = "DROP TABLE IF EXISTS dimTourist"
dimArriveDate_table_drop = "DROP TABLE IF EXISTS dimArriveDate"
#dimDepartDate_table_drop = "DROP TABLE IF EXISTS dimDepartDate"
factArrivetheUsa_table_drop = "DROP TABLE IF EXISTS factArrivetheUsa"

# CREATE TABLES

staging_immigration_table = ("""CREATE TABLE IF NOT EXISTS staging_immigration (
                                cicid float distkey,
                                i94citizenship TEXT,
                                i94resident TEXT,
                                Visa varchar(20) ,
                                i94modeOfEntry varchar(20),
                                i94AddressTo varchar(10),
                                i94PortOfEntryCity varchar(40), 
                                i94PortOfEntryState varchar(20),
                                gender varchar(10),
                                arrivedate date,
                                departdate date,
                                i94birthyear integer,
                                i94age integer,
                                fltno varchar(20),
                                airline varchar(10),
                                visatype varchar(10),
                                visapost varchar(10),
                                dtadfile varchar(10),
                                occup TEXT,
                                entdepa varchar(10),
                                entdepd varchar(10),
                                entdepu varchar(10),
                                matflag varchar(10),
                                dtaddto varchar(10),
                                insnum varchar(10),
                                admnum float,
                                PRIMARY KEY (cicid))
                                diststyle key;
                                """)

staging_airport_table = ("""CREATE TABLE IF NOT EXISTS staging_airport (
                                ident varchar(10),
                                type TEXT,
                                name TEXT,
                                iso_country varchar(10),
                                municipality TEXT,
                                gps_code varchar(10),
                                iata_code varchar(10),
                                local_code varchar(10),
                                statecode varchar(5),
                                latitude_deg varchar(25),
                                longitude_deg varchar(25),
                                coordinate varchar(50),
                                geocode varchar(400),
                                iso_region_state varchar(10),
                                ElevationFt integer,
                                City TEXT,
                                PRIMARY KEY (ident))
                                diststyle all;
                                """)


staging_demography_table =  ("""CREATE TABLE IF NOT EXISTS staging_demography(
                                StateCode varchar(10),
                                State TEXT,
                                MedianAge float,
                                TotalPopulation bigint,
                                NumberOfVeterans bigint,
                                AverageHouseholdSize float,
                                ForeignBorn bigint,
                                FemalePopulation bigint,
                                MalePopulation bigint,
                                PRIMARY KEY (StateCode))
                                diststyle all;
                                """)
                                
staging_landtemp_table = ("""CREATE TABLE IF NOT EXISTS staging_landtemp(
                                stagingtemp_id bigint,
                                dt date,
                                AverageTemperature float,
                                AverageTemperatureUncertainty float,
                                state varchar(20),
                                Country varchar(20),
                                PRIMARY KEY (stagingtemp_id))
                                diststyle all;
                                """)
                                
factArrivetheUsa_table =  ("""CREATE TABLE IF NOT EXISTS factArrivetheUsa ( 
                                arrive_id bigint identity(0,1),
                                Tourist_id float distkey,
                                arrivedate_id varchar(20) sortkey,
                                AddressTo varchar(5),
                                portOfEntryCity varchar(30),
                                departdate date,
                                arrivalFlag varchar(5),
                                departureFlag varchar(5),
                                updateFlag varchar(5),
                                matchFlag varchar(5),
                                admittedDate varchar(10),
                                PRIMARY KEY (arrive_id),
                                foreign key(Tourist_id) references dimTourist(Tourist_id),
                                foreign key(portOfEntryCity) references dimAirport(portOfEntryCity),
                                foreign key(arrivedate_id) references dimArriveDate(arrivedate_id),
                                foreign key(AddressTo) references dimStateDestination(AddressTo))
                                diststyle key; 
                                """)
                                

dimStateDestination_table = ("""CREATE TABLE IF NOT EXISTS dimStateDestination (
                                AddressTo varchar(5) sortkey,
                                StateCode varchar(5),
                                State varchar(20),
                                MedianAge float,
                                MalePopulation integer,
                                FemalePopulation integer,
                                TotalPopulation integer,
                                NumberOfVeterans integer,
                                ForeignBorn integer,
                                AverageHouseholdSize float,
                                PRIMARY KEY (AddressTo))
                                diststyle all;
                                """)
                            
dimTourist_table = ("""CREATE TABLE IF NOT EXISTS dimTourist (
                                Tourist_id float distkey,
                                gender varchar(5),
                                birthyear integer,
                                citizenship TEXT,
                                resident TEXT,
                                age integer,
                                occupation TEXT,
                                Visa varchar(20),
                                visatype varchar(10),
                                visapost varchar(10),
                                flight_no varchar(20),
                                airline varchar(10),
                                issuanceNum varchar(10),
                                admissionNum float,
                                PRIMARY KEY (Tourist_id))
                                diststyle key;
                                """)
    
dimAirport_table =  ("""CREATE TABLE IF NOT EXISTS dimAirport (
                                airport_id bigint identity(0,1),
                                airport_code varchar(10),
                                portOfEntryCity varchar(30) UNIQUE NOT NULL sortkey,
                                gps_code varchar(5),
                                iata_code varchar(5),
                                local_code varchar(5),
                                name TEXT,
                                type TEXT,
                                iso_country varchar(5),
                                municipality varchar(5),
                                statecode varchar(5),
                                ElevationFt integer,
                                PRIMARY KEY (airport_id))
                                diststyle all;
                                """)
    
dimArriveDate_table =   ("""CREATE TABLE IF NOT EXISTS dimArriveDate ( 
                            arrivedate_id varchar(20) sortkey,
                            year integer,
                            month integer,
                            day integer,
                            weekday integer,
                            week integer,
                            AverageTemperature float,
                            AverageTemperatureUncertainty float,
                            PRIMARY KEY (arrivedate_id))
                            diststyle all;
                            """)
    
    
    

                        
staging_immigration_copy = """copy staging_immigration from 's3://charlesproject-sink/immigrationData'
                            credentials 'aws_iam_role={}'
                            FORMAT AS PARQUET;""".format(*config['IAM_ROLE'].values())
staging_airport_copy = """copy staging_airport from 's3://charlesproject-sink/airportData'
                            credentials 'aws_iam_role={}'
                            FORMAT AS PARQUET;""".format(*config['IAM_ROLE'].values())
staging_demography_copy = """copy staging_demography from 's3://charlesproject-sink/demographyData'
                            credentials 'aws_iam_role={}'
                            FORMAT AS PARQUET;""".format(*config['IAM_ROLE'].values())
staging_landtemp_copy = """copy staging_landtemp from 's3://charlesproject-sink/landtempData'
                            credentials 'aws_iam_role={}'
                            FORMAT AS PARQUET;""".format(*config['IAM_ROLE'].values())
                        
                        
dimStateDestination_insert = ("""INSERT INTO dimStateDestination (AddressTo,StateCode,State,MedianAge,
                                    MalePopulation,FemalePopulation,TotalPopulation,
                                    NumberOfVeterans,ForeignBorn,AverageHouseholdSize)
                                        SELECT DISTINCT i.i94AddressTo as AddressTo,d.StateCode,d.State,d.MedianAge,
                                            d.MalePopulation,d.FemalePopulation,d.TotalPopulation,d.NumberOfVeterans,
                                            d.ForeignBorn,d.AverageHouseholdSize
                                            FROM staging_immigration as i
                                            LEFT JOIN staging_demography as d ON (d.StateCode = i.i94AddressTo);
                                            """)
        
dimTourist_insert = ("""INSERT INTO dimTourist (Tourist_id,gender,birthyear,citizenship,resident,age,occupation,
                                            Visa,visatype,flight_no,airline, issuanceNum , admissionNum)
                                 SELECT DISTINCT cicid as Tourist_id,gender,i94birthyear as birthyear,
                                     i94citizenship as citizenship,i94resident as resident,i94age as age,
                                     occup as occupation,Visa,visatype,fltno as flight_no,airline,
                                     insnum as issuanceNum,admnum as admissionNum
                                 FROM staging_immigration;
                                     """)
                        
                        
dimAirport_insert = ("""INSERT INTO dimAirport (airport_code, portOfEntryCity,gps_code,iata_code,
                                local_code,name,type,iso_country,municipality,statecode,ElevationFt) 
                                 SELECT DISTINCT a.ident as airport_code,
                                     i.i94PortOfEntryCity as portOfEntryCity,a.gps_code,
                                     a.iata_code,a.local_code,a.name,a.type,a.iso_country,
                                     a.municipality,a.statecode,a.ElevationFt
                                     FROM staging_immigration as i
                                     LEFT JOIN staging_airport as a ON (a.statecode = i.i94PortOfEntryCity);
                                     """)
                        
dimArriveDate_insert = ("""INSERT INTO dimArriveDate (arrivedate_id,year,month,day,
                            weekday,week,AverageTemperature,AverageTemperatureUncertainty)
                            SELECT DISTINCT cast(i.arrivedate AS text) AS arrivedate_id,
                                EXTRACT(year from (i.arrivedate)) AS year,EXTRACT(month from (i.arrivedate)) AS month,
                                EXTRACT(day from (i.arrivedate)) AS day,EXTRACT(weekday from (i.arrivedate)) AS weekday,
                                EXTRACT(week from (i.arrivedate)) AS week,l.AverageTemperature,
                                l.AverageTemperatureUncertainty
                            FROM staging_immigration as i
                            LEFT JOIN staging_landtemp as l ON (i.arrivedate = l.dt);
                            """)

factArrivetheUsa_insert = ("""INSERT INTO factArrivetheUsa (Tourist_id,arrivedate_id,AddressTo,
                                portOfEntryCity,departdate,arrivalFlag,departureFlag,updateFlag,matchFlag,admittedDate)
                                SELECT DISTINCT cicid as Tourist_id,cast(arrivedate AS text) AS arrivedate_id,
                                    i94AddressTo as AddressTo,i94PortOfEntryCity as portOfEntryCity,departdate,
                                    entdepa as arrivalFlag,entdepd as departureFlag,
                                    entdepu as updateFlag,matflag as matchFlag,dtaddto as admittedDate
                                FROM staging_immigration;
                                """)
                            
                                
                                

# QUERY LISTS

create_table_queries = [staging_immigration_table,staging_airport_table,staging_demography_table,
                        staging_landtemp_table,dimStateDestination_table,dimTourist_table,dimAirport_table,
                        dimArriveDate_table,factArrivetheUsa_table]
drop_table_queries = [staging_immigration_table_drop,staging_airport_table_drop,staging_demography_table_drop,                                   staging_landtemp_table_drop,factArrivetheUsa_table_drop,dimStateDestination_table_drop,
                     dimAirport_table_drop,dimTourist_table_drop,dimArriveDate_table_drop]

copy_table_queries = [staging_immigration_copy,staging_airport_copy,staging_demography_copy,staging_landtemp_copy]
insert_table_queries = [dimStateDestination_insert,dimTourist_insert,dimAirport_insert,dimArriveDate_insert,
                        factArrivetheUsa_insert]



#QUALITY CHECK

test_duplicate = """SELECT {column},count({column}) AS duplicates FROM {table} group By {column}
                            HAVING count({column}) > 1;"""

test_null = """SELECT {column} FROM {table} WHERE {column} IS NULL;"""

test_invalid_record = """SELECT SUM(CASE WHEN CAST(arrivedate_id AS date) > departdate THEN 1 ELSE 0 END) AS
                    invalid_records FROM factArrivetheUsa;"""

test_record_complete = """SELECT count(*) FROM {table};"""