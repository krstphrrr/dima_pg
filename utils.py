import pyodbc

class Acc:
    con=None
    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima
        DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
        self.con = pyodbc.connect(mdb_string)

    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)
