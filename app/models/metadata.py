from app.models.base import Base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Date, DateTime, String


class ETL_Metadata(Base):
    __tablename__ = "etl_metadata"

    table = Column(String(255), primary_key=True)
    last_date_processed = Column(Date, nullable=False)
    last_run_date_time = Column(DateTime, nullable=False)

    def __init__(self, table, last_date_processed, last_run_date_time):
        self.table = table
        self.last_date_processed = last_date_processed
        self.last_run_date_time = last_run_date_time
