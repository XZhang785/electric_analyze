# -*- coding: utf-8 -*- 
# @Time : 2023/3/31 10:04 
# @Author : zhangqinming
# @File : Client.py

import traceback
import sys
from typing import List, Optional
from hdfs import InsecureClient
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException


def check_file_suffix(file: str, suffix: str) -> bool:
    if file.endswith('.' + suffix):
        return True
    else:
        return False


class HDFSClient:
    DIR_TYPE = 'DIRECTORY'
    FILE_TYPE = 'FILE'
    base_dir = '/electric-analyse/data'

    def __init__(self, hdfs_url: str, user: str):
        self.hdfs = InsecureClient(hdfs_url, user)

    def existed(self, path: str) -> bool:
        """
        判断HDFS上是否存在文件或文件夹
        :param path: 文件或文件夹的绝对路径
        :return: 存在返回True，不存在返回False
        """
        status = self.hdfs.status(path, strict=False)
        if status is not None:
            return True
        else:
            return False

    def get_path_type(self, path: str) -> str:
        """
        获取文件的类型
        :param path: 文件或文件夹的绝对路径
        :return: 如果文件在HDFS上，会返回文件的类型，否则返回’error_type‘
        """
        status = self.hdfs.status(path, strict=False)
        if status is not None:
            return status['type']
        else:
            return 'error_type'

    def list_hdfs(self) -> List[str]:
        """
        列出当前目录下的所有文件名
        :return: 以列表的形式返回当前目录下的文件名
        """
        return self.hdfs.list(self.base_dir)

    def choose_dir(self, path: str) -> None:
        """
        选择当前目录的下一级目录，如果该目录存在，则会改变当前的目录
        :param path: 文件名
        :return: 空
        """
        if self.get_path_type(path) == self.DIR_TYPE:
            self.base_dir = self.base_dir + "/" + path
            print('the base dir is turn to ' + path)
        else:
            print('choose dir ' + path + ' error!!')

    def choose_file(self, file_name: str) -> Optional[str]:
        """
        选择当前目录下的文件
        :param file_name: 在当前目录下的文件名
        :return: 返回文件的绝对路径
        """
        file_path = self.base_dir + '/' + file_name
        if self.get_path_type(file_path) == self.FILE_TYPE:
            return file_path
        else:
            print(file_name, " is not exist")
            return None

    def change_base_dir(self, path: str) -> None:
        """
        更改工作文件夹
        :param path: 文件夹的绝对路径
        :return: None
        """
        if self.get_path_type(path) == self.DIR_TYPE:
            self.base_dir = path
            print('the base dir is change to ' + path)
        else:
            print('change dir ' + path + ' error!!')


class SparkClient:
    def __init__(self, spark_master: str = "local", spark_log_level: str = "Error"):
        self.spark = SparkSession.builder.master(spark_master).getOrCreate()
        self.spark.sparkContext.setLogLevel(spark_log_level)

    def read_data(self, path: str, infer_schema: bool = True, header: bool = True) -> Optional[DataFrame]:
        """
        从HDFS中读取数据,csv格式的结构化数据,可以直接读取csv文件，也可以读取它一级目录下所有csv文件，并合并成一个DataFrame

        :param path: 文件或文件夹的绝对路径，要求要以.csv为后缀
        :param infer_schema: 是否要推断出模式，默认为要
        :param header: 是否将数据第一行视为列名，默认为要

        :return: SparkDataFrame 或 None
        """
        try:
            if check_file_suffix(path, 'csv'):
                data = self.spark.read.csv(path, inferSchema=infer_schema, header=header)
                return data
            else:
                print(path, ' is not in csv format')
                return None
        except AnalysisException:
            print("path " + path + " does not exist")
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=None, file=sys.stdout)

    def write_data(self, data: DataFrame, path: str, header: bool = True, mode: str = "overwrite") -> None:
        """
        持久化DataFrame到HDFS中，以csv格式保存在文件中

        :param data: 要进行持久化的数据
        :param path: 输出文件的绝对路径，要求路径以csv为后缀
        :param header: 是否要保存列名， 默认为要
        :param mode: 写的模式，默认为覆写

        :return: None
        """
        try:
            if check_file_suffix(path, 'csv'):
                data.write.mode(mode).csv(path, header=header)
            else:
                print(path, ' is not in csv format')
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=None, file=sys.stdout)

    def close_client(self) -> None:
        """
        关闭客户端连接

        :return: None
        """
        self.spark.stop()
        print("successfully close the spark client")
