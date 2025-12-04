# 配置文件
import os

class Config:
    # Spark配置
    SPARK_APP_NAME = "MovieLensALS"
    SPARK_MASTER = "local[2]"
    
    # MySQL配置
    MYSQL_HOST = "localhost"
    MYSQL_PORT = 3306
    MYSQL_DATABASE = "movierecommend"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "123456"
    
    # JDBC URL
    @property
    def mysql_url(self):
        return f"jdbc:mysql://{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
    
    # 数据表名
    TABLE_PERSONAL_RATINGS = "personalratings"
    TABLE_RECOMMEND_RESULT = "recommendresult"
    
    # 模型参数
    RANKS = [8, 12]  # 隐语义因子个数
    LAMBDAS = [0.1, 10.0]  # 正则化参数
    NUM_ITERS = [10, 20]  # 迭代次数
    NUM_PARTITIONS = 4
    
    # 数据文件路径（根据您的设置更新）
    DATA_DIR = "/home/zsh/ml-latest-small"
    RATINGS_FILE = "ratings.csv"
    MOVIES_FILE = "movies.csv"

config = Config()