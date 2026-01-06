"""
公共异常定义模块

定义 ETL Pipeline 架构中使用的所有自定义异常类
"""


class PipelineException(Exception):
    """Pipeline 基础异常类"""
    pass


class CollectorException(PipelineException):
    """采集器异常"""
    pass


class TransformerException(PipelineException):
    """转换器异常"""
    pass


class LoaderException(PipelineException):
    """加载器异常"""
    pass


class ConfigException(PipelineException):
    """配置异常"""
    pass


class ValidationException(PipelineException):
    """数据验证异常"""
    pass


class DataSourceException(CollectorException):
    """数据源异常"""
    pass


class NetworkException(CollectorException):
    """网络异常"""
    pass


class DatabaseException(LoaderException):
    """数据库异常"""
    pass

