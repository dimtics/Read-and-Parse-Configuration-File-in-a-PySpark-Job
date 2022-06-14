from pyspark.sql import SparkSession
from pyspark import SparkFiles
from configparser import ConfigParser, ExtendedInterpolation
import subprocess


def getConfigFromS3(configFileS3Path: str):
    """ A function to read and parse config file in a PySpark Job.
    Args:
        configFileS3Path (str): Path of the configuration file placed in an S3 bucket.
    Returns:
        ConfigParser object from which config values can be extracted using get() call.
    """

    try:
        # copy config file from s3 bucket to spark tmp directory
        cmdx = f"""aws s3 cp {configFileS3Path} /tmp"""
        cmd_result = subprocess.run(
            ["/bin/bash"], input=cmdx, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

        # if copy file is successful
        if cmd_result.returncode == 0:

            # get config file name from s3 path
            objkey = configFileS3Path.split("/")[-1]

            # add config file to spark driver and executors
            spkContext = SparkSession.getActiveSession().sparkContext
            spkContext.addFile(f"/tmp/{objkey}")

            # get the absolute path to the config file
            configFilePath = SparkFiles.get(objkey)

            # parse the config file
            config = ConfigParser(interpolation=ExtendedInterpolation())
            config.read(configFilePath)

            # return configparser object
            return config

        else:
            print(
                f"Copy config file from s3 bucket to spark /tmp directory failed!. This is the error message: {cmd_result.stderr}")

    except Exception as err:
        print(err)
