def maketableview_pg(in_table,nameout):
    import psycopg2, re
    from psycopg2 import sql
    from temp_tools import config
    params = config()
    con1 = psycopg2.connect(**params)
    cur1 = con1.cursor()
    cur1.execute(
        sql.SQL("CREATE TABLE postgres.public.{} AS SELECT"
        )


class all_pg():

    def __init__(self, print_SQL):
        from temp_tools import config, dconfig
        import pyodbc,psycopg2,os
        dparams = dconfig()
        MDB = 'C:\\Users\\kbonefont.JER-PC-CLIMATE4\\Desktop\\Some_data\\db.mdb'
        DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)

        self.access_cur = pyodbc.connect(mdb_string).cursor()
        self.pg_con = psycopg2.connect(**dparams)
        self.pg_cur = self.pg_con.cursor()

        self.print_SQL = print_SQL

        self.schema_name = self.get_access_db_name()

    def get_access_db_name(self):

        for table in self.access_cur.tables():
            import os
            return os.path.splitext(os.path.basename(table.table_cat))[0]

    def create_schema(self):

        SQL = """
        CREATE SCHEMA "{schema_name}"
        """.format(schema_name=self.schema_name)

        if self.print_SQL:
            print(SQL)

        self.pg_cur.execute(SQL)
        self.pg_con.commit()

    def create_tables(self):

        # Generate list of tables in schema
        table_list = list()
        for table in self.access_cur.tables():
            if table.table_type == "TABLE":
                table_list += [table.table_name, ]

        for table in table_list:
            SQL = """
            CREATE TABLE "{schema}"."{table}"
            (
            """.format(schema=self.schema_name, table=table)

            SQL += self.create_fields(table)

            SQL += """
            ) """

            if self.print_SQL:
                print(SQL)

            self.pg_cur.execute(SQL)
            self.pg_con.commit()

    def create_fields(self, table):

        postgresql_fields = {
            'COUNTER': 'serial',  # autoincrement
            'VARCHAR': 'text',  # text
            'LONGCHAR': 'text',  # memo
            'BYTE': 'integer',  # byte
            'SMALLINT': 'integer',  # integer
            'INTEGER': 'bigint',  # long integer
            'REAL': 'real',  # single
            'DOUBLE': 'double precision',  # double
            'DATETIME': 'timestamp',  # date/time
            'CURRENCY': 'money',  # currency
            'BIT':  'boolean',  # yes/no
        }

        SQL = ""
        field_list = list()
        for column in self.access_cur.columns(table=table):
            if column.type_name in postgresql_fields:
                field_list += ['"' + column.column_name + '"' +
                               " " + postgresql_fields[column.type_name], ]
            elif column.type_name == "DECIMAL":
                field_list += ['"' + column.column_name + '"' +
                               " numeric(" + str(column.column_size) + "," +
                               str(column.decimal_digits) + ")", ]
            else:
                print("column " + table + "." + column.column_name +
                " has uncatered for type: " + column.type_name)

        return ",\n ".join(field_list)
