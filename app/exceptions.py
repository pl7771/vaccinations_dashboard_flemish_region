class IncorrectDataSourcePath(Exception):
    """Exception raised for errors in the data source path

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, salary, message="The path of datasource is incorrect"):
        self.message = message
        super().__init__(self.message)
