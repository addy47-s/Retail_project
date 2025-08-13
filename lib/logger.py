from pyspark.sql import SparkSession

class Log4j:
    """
    Wrapper class for Log4j logging from Spark applications.
    Allows logging messages at different levels using Spark's internal JVM logger.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the Log4j logger using the Spark JVM context.

        :param spark: Active SparkSession
        """
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("retail_analysis")

    def info(self, message: str):
        """
        Log an info-level message.

        :param message: The message string to log
        """
        self.logger.info(message)

    def warn(self, message: str):
        """
        Log a warning-level message.

        :param message: The message string to log
        """
        self.logger.warn(message)

    def error(self, message: str):
        """
        Log an error-level message.

        :param message: The message string to log
        """
        self.logger.error(message)
