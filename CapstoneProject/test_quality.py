from quality_check import check_duplicate, check_null, check_invalid_record, check_record_complete, close_conn

# dictionary of expected table counts and table names for test_check_record_complete arguments
# {'staging_immigration':2994504,'staging_airport': 1041,'staging_demography': 49,'staging_landtemp':141930}



#dictionary of primary_keys and table_names to test with
# {'AddressTo':'dimStateDestination','Tourist_id':'dimTourist','airport_id':'dimAirport','arrivedate_id':'dimArriveDate',
#'arrive_id':'factArrivetheUsa'}


def test_check_duplicate(primary_key= 'arrivedate_id',table_name ='dimArriveDate'):
    assert(check_duplicate(primary_key,table_name) == [])

    
def test_check_null(primary_key= 'AddressTo',table_name ='dimStateDestination'):
    assert(check_null(primary_key,table_name) == [])
    
# 268 rows where invalid. see example.ipynb for the invalid records

def test_check_invalid_record():
    assert(check_invalid_record() == 268)
    
def test_check_record_complete(table_name ='factArrivetheUsa',count = 2994504):
    assert(check_record_complete(table_name) == count)
    
def test_close_conn():
    assert(close_conn() == True)
