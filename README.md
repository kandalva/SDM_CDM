# SDM_CDM
DDL Generator and DDL for Semantic Data Modeling v.1.04
=================

The tool generates DDL files for SDM DWH. SDM is an abbreviation of “Semantic Data Model”, designed for standardization of DWH to share cohort query and BI tools experiences in the healthcare community. See details about SDM at the SDM consortium (http://sdm-c.org/).

If you want to get full Common Data Model(CDM) specification of SDM, please consider join or subscribe the specification from the SDM consortium.

Currently, it is confirmed that the script generates DDL for SQLite3, MySQL, PostgreSQL, DB2, Oracle, Microsoft SQL. The DDL of RDBMS supported by sqlalchemy library and plugin may be generated by customizing the supported_rdbms list. [Don’t forget to install ]

If you simply want to build SDM-based DWH, you need not run the tool, however, you may copy DDL file suitable for your specific environment from the 'sql' directory.

Installation
==================

pip install numpy pandas
pip install pre sqlalchemy #for comment feature supported by SQLAlchemy 1.2 (beta at Sep 2017)
pip install ibm_db_sa #(optional) for who wants DB2 support.

Place SDM schema definition file on the specified folder(../specs) of the script and run with:

python export.py

It will generate DDL files of SDM DWH in sql folder for RDBMS implements supported by sqlalchemy.

