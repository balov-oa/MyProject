from functools import lru_cache
from urllib import parse

from sqlalchemy import create_engine


@lru_cache()
def get_sqlalchemy_engine():
    params = parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                              "SERVER=OLEG;"
                              "DATABASE=Apartment_Tomsk;"
                              "Trusted_Connection=yes")

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine
