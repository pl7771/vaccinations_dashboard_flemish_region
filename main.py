import argparse
import os
import ssl

from alembic.config import Config
from alembic import command

from app import utils
from app import _app
from app.logger import get_logger

ssl._create_default_https_context = ssl._create_unverified_context
logger = get_logger(__name__)

def run():
    return _app.run()


def run_migrations(downgrade_first=False):
    # run db migrations
    db_type = utils.get_db_type()
    logger.info("starting to run the migrations for '{db_type}'...".format(db_type=db_type))
    with utils.db_session() as session:
        alembic_cfg = Config("alembic.ini")
        # pylint: disable=unsupported-assignment-operation
        alembic_cfg.attributes["connection"] = session.bind

        if downgrade_first:
            command.downgrade(
                alembic_cfg, "base"
            )  # doet een downgrade vd db en gooit alles weg

        command.upgrade(alembic_cfg, "head")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the covid data ETL job.")
    parser.add_argument(
        "-m",
        "--migrate",
        help="Do you want to run database migrations at first?",
        choices=["upgrade", "downgrade_first"],
    )
    # Parsing YAML file
    args = parser.parse_args()
    if args.migrate:
        run_migrations(downgrade_first=(args.migrate == "downgrade_first"))
    run()
