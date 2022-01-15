from datetime import date, datetime, timedelta, tzinfo
from pytz import timezone
import tqdm

import pandas as pd

from app.etl.transformer import Transformer
from app.exceptions import IncorrectDataSourcePath
from app.logger import get_logger
from app.models.base import Base
from app.models.metadata import ETL_Metadata
from app.utils import db_session, get_db_type

logger = get_logger(__name__)
class Pipeline:
    def __init__(
        self,
        data_class: Base,
        path: str,
        transformer: Transformer,
        metadata_handler=None,
    ):
        self.data_class = data_class
        self.path = path
        self.transformer = transformer
        self.metadata_handler = metadata_handler

    def extract(self):
        """Haalt data op van url

        Returns:
            pd.DataFrame: [description]
        """
        if self.metadata_handler:
            etl_metadata=None
            with db_session(echo=False) as session:
                etl_metadata = get_etl_metadata(session, (self.data_class).__tablename__)
                session.close()
            if "frequency" in dict.keys(self.metadata_handler):
                frequency = self.metadata_handler["frequency"]
                if frequency == "yearly":
                    if etl_metadata:
                        # return empty dataframe when pipeline has already exported the date of current year
                        if etl_metadata.last_date_processed >= datetime.now(
                            timezone("Europe/Brussels")
                        ).date().replace(month=1, day=1):
                            return pd.DataFrame()
                # if frequency == "daily":
                #     if "full_refresh" in dict.keys(self.metadata_handler):
                #         full_refresh = self.metadata_handler["full_refresh"]
                #         if full_refresh:
                #             if etl_metadata:
                #                 # return empty dataframe when pipeline has already exported the date of today
                #                 if (
                #                     etl_metadata.last_date_processed
                #                     >= datetime.now(timezone("Europe/Brussels")).date()
                #                 ):
                #                     return pd.DataFrame()

        if ".csv" in self.path:
            data_frame = pd.read_csv(self.path)
        elif ".xlsx" in self.path:
            data_frame = pd.read_excel(self.path, nrows=50)
        elif ".zip" in self.path:
            data_frame = pd.read_csv(self.path, delimiter="|")
        else:
            raise IncorrectDataSourcePath
        return data_frame

    def transform(self, data_frame: pd.DataFrame):
        """Transformeert de data in correct formaat

        Args:
            data_frame (pd.DataFrame): [description]

        Returns:
            [type]: [description]
        """
        if data_frame.empty:
            return data_frame
        return self.transformer.transform(data_frame)

    def handle_metadata(self, data_frame: pd.DataFrame):
        self.last_date_processed = date.min

        with db_session(echo=False) as session:
            # fetch the 'etl_metadate' from the database for the pipeline and set it's 'last_date_processed' as a variable
            etl_metadata = get_etl_metadata(session, (self.data_class).__tablename__)
            if etl_metadata:
                self.last_date_processed = etl_metadata.last_date_processed
            session.close()

        # When there is no 'metadata_handler' registered for the pipeline, just return the dataframe as is.
        if not self.metadata_handler:
            return data_frame

         # When the data_frame is empty, just return the dataframe as is.
        if data_frame.empty:
            return data_frame

        if "full_refresh" in dict.keys(self.metadata_handler):
            full_refresh = self.metadata_handler["full_refresh"]
            if full_refresh:
                self.last_date_processed = datetime.now(
                    timezone("Europe/Brussels")
                ).date()

        if self.metadata_handler:
            # When a 'date_column' is provided in the 'metadata_handler', filter the dataframe
            if "date_column" in dict(self.metadata_handler):
                date_until_to_filter = datetime.now(timezone("Europe/Brussels")).date()
                data_frame = data_frame[
                    (
                        data_frame[self.metadata_handler["date_column"]]
                        > self.last_date_processed
                    )
                    & (
                        data_frame[self.metadata_handler["date_column"]]
                        <= date_until_to_filter
                    )
                ]
            # When a 'year_column' is provided in the 'metadata_handler', filter the dataframe
            if "year_column" in dict(self.metadata_handler):
                year_until_to_filter = datetime.now(timezone("Europe/Brussels")).date().year
                data_frame = data_frame[
                    (
                        data_frame[self.metadata_handler["year_column"]]
                        > self.last_date_processed.year
                    )
                    & (
                        data_frame[self.metadata_handler["year_column"]]
                        <= year_until_to_filter
                    )
                ]
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        last_run_date_time = datetime.now(timezone("Europe/Brussels"))
        # self.last_date_processed set during 'handle_metadata'
        last_date_processed = self.last_date_processed

        if data_frame.empty:
            with db_session() as session:
                etl_metadata = update_or_set_etl_metadata(
                    session,
                    (self.data_class).__tablename__,
                    last_run_date_time,
                    # last_date_processed
                )
                session.commit()
                session.close()
            return []

        data_list = [
            self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
        ]

        truncate_table = False
        if self.metadata_handler:
            if "date_column" in dict.keys(self.metadata_handler):
                date_column = self.metadata_handler["date_column"]
                last_date_processed = (data_frame[date_column]).max()
            if "year_column" in dict.keys(self.metadata_handler):
                date_column = self.metadata_handler["year_column"]
                last_date_processed = datetime(
                    (data_frame[date_column]).max(),
                    12,
                    31,
                    tzinfo=timezone("Europe/Brussels")
                ).date()
            if "full_refresh" in dict.keys(self.metadata_handler):
                full_refresh = self.metadata_handler["full_refresh"]
                if full_refresh:
                    truncate_table = True
        with db_session() as session:
            if truncate_table:
                execute_truncate_table(session, (self.data_class).__tablename__)
            etl_metadata = update_or_set_etl_metadata(
                session,
                (self.data_class).__tablename__,
                last_run_date_time,
                last_date_processed
            )
            # This log will only be process when the log level of the 'sqlalchemy' logger in the 'logger.ini' is set to 'INFO' or less.
            logger.info(
                "pipeline '{table}' : {length} lines to be added to the database. etl_metadata.last_date_processed will be set to {last_update}".format(
                    table=self.data_class.__tablename__,
                    length=len(data_list),
                    last_update=etl_metadata.last_date_processed,
                )
            )

            # engine = session.get_bind()
            # data_frame.to_sql(
            #     (self.data_class).__tablename__,
            #     con=engine,
            #     if_exists="append",
            #     chunksize=1000,
            #     index=False
            # )
            length = len(data_list)
            step = 1000
            array_to_process = []
            processed = 0
            index = 0
            while index < length:
                till_index = index + step
                if till_index > length:
                    till_index = length
                array_to_process.append(data_list[index:till_index])
                index = till_index

            processed = 0
            # for i in array_to_process:
            # https://pypi.org/project/tqdm/
            for i in tqdm.tqdm(array_to_process, desc=self.data_class.__tablename__):
                session.bulk_save_objects(i)
                processed = processed + len(i)
                # This log will only be process when the log level of the 'sqlalchemy' logger in the 'logger.ini' is set to 'INFO' or less.
                # logger.info(
                #     "pipeline '{table}' : {processed} of {length} lines added to the database...".format(
                #         table=self.data_class.__tablename__,
                #         processed=processed,
                #         length=len(data_list),
                #     )
                # )
            # session.bulk_save_objects(data_list)
            session.commit()
        return data_list

    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_frame = self.handle_metadata(data_frame)
        data_list = self.load(data_frame)
        return data_list


def get_etl_metadata(session, tablename):
    etl_metadata = (
        session.query(ETL_Metadata)
        .filter(
            (ETL_Metadata.table == tablename)
        )
        .first()
    )
    return etl_metadata

def update_or_set_etl_metadata(session, tablename, last_run_date_time, last_date_processed=None):
    etl_metadata = get_etl_metadata(session, tablename)
    if etl_metadata:
        if (last_date_processed):
            etl_metadata.last_date_processed = last_date_processed
        etl_metadata.last_run_date_time = last_run_date_time
    else:
        etl_metadata = ETL_Metadata(
            tablename,
            last_date_processed or date.min,
            last_run_date_time
        )
        session.add(etl_metadata)
    return etl_metadata

def execute_truncate_table(session, tablename):
    session.execute(
        (
            ("""TRUNCATE TABLE {tablename}""").format(
                tablename=tablename
            )
        ) if (get_db_type() == "mssql") else (
            ("""DELETE FROM {tablename}""").format(
                tablename=tablename
            )
        )
    )
