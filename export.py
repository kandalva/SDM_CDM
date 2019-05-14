# -*- coding: utf-8 -*-

import os
import pandas as pd
import re
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.schema import UniqueConstraint

import sqlalchemy.types  # for SQL data types


debug = False # print out generated SQL statment without saving file.

#temporary drop ibm_db2 support for my shortage of time
supported_rdbms = ['sqlite', 'mysql', 'postgresql','oracle', 'mssql']
#supported_rdbms = ['db2+ibm_db','sqlite', 'mysql', 'postgresql','oracle', 'mssql']
maximum_idx_length = {
    'db2+ibm_db' : 64,
    'sqlite' : 64,
    'mysql' : 64,
    'postgresql':63,
    'oracle':30,
    'mssql':64
}


#SDM Common Data Model specification file
spec_file = '../specs/SDM_V1.09.xlsx'


class SQLCompiler:
    def __init__(self, db):
        self.db_url = db + '://mock'
        self.engine = create_engine(self.db_url, strategy='mock', executor=self.dump)

    def dump(self, sql, *multiparams, **params):
        self.stmt = self.stmt + str(sql.compile(dialect=self.engine.dialect)) + ";\n"

    def compile(self, metadata):
        self.stmt = ""
        metadata.create_all(self.engine)
        return self.stmt


xl = pd.ExcelFile(spec_file)

sheets = [x for x in xl.sheet_names if 'SDM_' in x]

key_values = ['P', 'N', '0', 'K', '',' ']
index_values = ['I', 'N', 'M', '',' ']

re_nullables = re.compile(r"^NOT NULL"),

re_matchings = {
    "CHAR": re.compile(r"^CHAR\((.+?)\)"),
    "VARCHAR": re.compile(r"^VARCHAR\((.+?)\)"),
    "TIMESTAMP": re.compile(r"^TIMESTAMP"),
    "INT": re.compile(r"^INT"),
    "REAL": re.compile(r"^REAL"),
    "DATE": re.compile(r"^DATE"),
    "TIME": re.compile(r"^TIME"),
    "CLOB": re.compile(r"^CLOB"), #Ignore size options for using default size of CLOB
    "BLOB": re.compile(r"^BLOB")
}

obj_mapper = {
    "CHAR": lambda x: sqlalchemy.types.CHAR(x),
    "VARCHAR": lambda x: sqlalchemy.types.VARCHAR(x),
    "TIMESTAMP": lambda x: sqlalchemy.types.TIMESTAMP,
    "INT": lambda x: sqlalchemy.types.INT,
    "REAL": lambda x: sqlalchemy.types.REAL,
    "DATE": lambda x: sqlalchemy.types.DATE,
    "TIME": lambda x: sqlalchemy.types.TIME,
    "CLOB": lambda x: sqlalchemy.types.TEXT,
    "BLOB": lambda x: sqlalchemy.types.LargeBinary,
    "TEXT": lambda x: sqlalchemy.types.TEXT
}

for rdbms in supported_rdbms:

    compiler = SQLCompiler(rdbms)
    sql_stmts = ""

    for sheet in sheets:
        metadata = MetaData()
        tbl = Table(sheet, metadata)

        df = xl.parse(sheet)
        df = df.replace(np.nan, '', regex=True)
        df['KEY'] = df['KEY'].apply(str)
        df['INDEX'] = df['INDEX'].apply(str)

        for index, row in df.iterrows():
            def_vartype = row['型']
            def_nullable = row['NULL']
            comment = "" # comment = row['項目の内容'] # Intentionally removed comment for concerning intelectural property issue of SDM specification.
            vartype = None
            varlength = 0
            varname = row['項目（英語）']
            if varname == '':
                varname = 'NA' #pandas parse 'NA' to ''を値無しに解釈する。NAフィルタをExcelFileメソッドで無効にする方法は？
            nullable = True
            varkey = row['KEY']
            varindex = row['INDEX']
            is_primary_key = False

            for k, v in re_matchings.items():
                m = v.search(def_vartype)
                if m:
                    vartype = k
                    if len(m.groups()) > 0:
                        varlength = int(m.group(1))
                    break



            # Check Key options
            if row['KEY'] == 'P':
                is_primary_key = True

            # Check every items are parsed correctly
            if vartype == None:
                print("@ %(sheet)s %(varname)s - %(def_nullable)s " % locals())
                raise

            if varkey not in key_values:
                print("@ %(sheet)s %(varname)s - %(varkey)s " % locals())
                raise

            if varindex not in index_values:
                print("@ %(sheet)s %(varname)s - %(varindex)s " % locals())
                raise

            if def_nullable in ['NOT_NULL','NOT NULL']:
                nullable = False
            elif def_nullable in ['','　']:
                pass
            else:
                raise ValueError("@ %(sheet)s %(varname)s - %(def_nullable)s " % locals())

            #Check Index options
            #I : Normal Index


            if row['INDEX'] == 'I':
                is_indexed = True
            else:
                is_indexed = False

            #M : Multi-Dimensional Cluster Index

            #長すぎるCHAR/VARCHARはTEXTへ 基準として256文字
            if (vartype in ['CHAR','VARCHAR'] and varlength >= 256):
                obj = obj_mapper["TEXT"](varlength)
            else:
                obj = obj_mapper[vartype](varlength)
            col = Column(varname, obj, primary_key=is_primary_key,nullable=not nullable,index=is_indexed)
            col.comment = comment # remove comment
            tbl.append_column(col)


        #Check mulitple key options
        dfk = df[df['KEY'] == 'K']
        if len(dfk) > 1:
            idx_name = ("uniq_idx_"+'_'.join(dfk['項目（英語）']))[:maximum_idx_length[rdbms]] #Issue: MySQL has maximum 64 chars for index name
            tbl.append_constraint(UniqueConstraint(*dfk['項目（英語）'],name=idx_name))


        sql_stmt = compiler.compile(metadata)
        sql_stmts = sql_stmts + sql_stmt + "\n"

    if debug:
        print(sql_stmts)
    else:
        file = "./sql/"+rdbms+".sql"
        if os.path.exists(file):
            os.remove(file)
        f = open(file, 'w',encoding="utf-8")
        f.write(sql_stmts)
        f.close()

