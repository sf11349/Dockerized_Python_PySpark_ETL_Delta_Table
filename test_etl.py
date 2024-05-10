from etl import *
from etl_stages.extract import extract
from etl_stages.transform import transform
from etl_stages.load import load

class TestETL:
    
    def test_extract(self):
        try:
            extract()
            assert True
        except Exception as e:
            print (e)
            assert False
    
    def test_transform(self):
        try:
            transform()
            assert True
        except Exception as e:
            print (e)
            assert False
    
    def test_load(self):
        try:
            load()
            assert True
        except Exception as e:
            print (e)
            assert False