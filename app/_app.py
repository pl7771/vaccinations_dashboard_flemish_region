import os
import json

from app.etl.pipeline import Pipeline
from app.etl.transformer import Transformer
from app.models import models
from app.settings import BASE_DIR
from app.logger import get_logger
from app.utils import send_mail

class Message():
    def __init__(self, success, model, message):
        self.success = success
        self.model = model
        self.message = message

def run():
    logger = get_logger(__name__)

    with open(
        os.path.join(BASE_DIR, "app/data.json"),
        mode="r",
        encoding="UTF-8"
        # os.path.join(BASE_DIR, "app/data_test.json"), mode="r", encoding="UTF-8"
    ) as file:
        data = json.load(file)
        success=True
        messages=[]
        for pl in data["pipelines"]:
            try:
                data_class = getattr(models, pl["model"])
                metadata_handler = None
                if "metadata_handler" in dict.keys(pl):
                    metadata_handler = pl["metadata_handler"]
                pipeline = Pipeline(
                    data_class,
                    path=pl["source"],
                    transformer=Transformer(pl["tranforms"]),
                    metadata_handler=metadata_handler,
                )
                # data_frame = pipeline.extract()
                # data_frame = pipeline.transform(data_frame)
                # data_frame = pipeline.handle_metadata(data_frame)
                # # print(data_frame)
                # # print(data_frame.describe())
                # print(data_frame.info())
                # return
                logger.info("Starting to process pipeline '{table}'...".format(
                    table=data_class.__tablename__
                ))
                data_list = pipeline.process()
                message="Finished pipeline '{table}' : {length} lines added to the database".format(
                    table=data_class.__tablename__, length=len(data_list)
                )
                messages.append(
                    Message(
                        success=True,
                        model=data_class.__tablename__,
                        message=message
                    )
                )
                logger.info(message)
            except Exception as e:
                model="Unknown"
                if (type(pl) == dict):
                    if ("model" in dict.keys(pl)):
                        model = pl["model"] 
                message="Pipeline '{model}' : {exception}".format(
                    model=model,
                    exception=e
                )
                success=False
                messages.append(
                    Message(
                        success=False,
                        model=model,
                        message=message
                    )
                )
                logger.error(message, exc_info=True)

        send_info(success, messages)

def send_info(success, messages):
    html_content="""
        <p>Dear,</p>
        <p>Here you can find more information about the pipelines.</p>
        <table style="width:100%;">
    """
    html_content=html_content+"""
        <thead>
            <tr>
                <th>Success</th>
                <th>Pipeline</th>
                <th>Message</th>
            </tr>
        </thead>
        <tbody>
    """
    for message in messages:
        html_content=html_content+"""
            <tr>
                <td>{message.success}</td>
                <td>{message.model}</td>
                <td>{message.message}</td>
            </tr>
        """.format(message=message)
    html_content=html_content+"""
            </tbody>
        </table>
    """
    send_mail(
        subject="SUCCESS | ETL for covid pipelines processed successfully" if success else "ERROR | ETL for covid pipelines processed with errors...",
        html_content=html_content
    )

