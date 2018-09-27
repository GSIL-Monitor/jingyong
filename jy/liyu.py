import pandas as pd
from sqlalchemy import create_engine
import pymysql
import autosklearn



def get_file(file_path,header=None,names=None,sheet_name=None):
    """
    读取xlsx或者csv文件
    :param file_path:  文件存放路径
    :param header:  表头,默认为None，表示源数据无表头，=0表示第一行是表头
    :param names: 对应header=0时，可自行设定表头
    :param sheet_name: 默认为None，如果是读取excel表中的特定sheet名，可传入。否则读取默认的
    :return: 返回dataframe
    """
    if file_path[-3:]=='csv':
        file_data=pd.read_csv(file_path,header=header,names=names)
    elif file_path[-4:]=='xlsx':
        file_data=pd.read_excel(file_path,sheet_name=sheet_name,header=header,names=names)

    return file_data

def data_clean():
    """
    数据预处理
    :return:
    """


def choice_function(dataframe,y_label=None):
    """
    选择回归还是分类
    :param dataframe: 经过数据预处理之后的dataframe
    :param y_label:  标签列
    :return: 结果
    """
    kind=len(set(dataframe[y_label].tolist()))
    if kind>5:
        result=classifier()
    else:
        result =regression()

    return result


def classifier():
    """
    分类算法
    :return:
    """
    print('classifier')

def regression():
    """
    回归算法
    :return:
    """
    print('regression')

