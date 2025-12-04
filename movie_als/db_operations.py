# 数据库操作模块
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import mysql.connector
from config import config

class DatabaseOperations:
    def __init__(self):
        self.spark = None
    
    def init_spark(self):
        """初始化Spark会话"""
        self.spark = SparkSession.builder \
            .appName("DBOperations") \
            .master(config.SPARK_MASTER) \
            .getOrCreate()
        return self.spark
    
    def delete_user_recommendations(self, user_id):
        """删除用户的旧推荐结果"""
        try:
            conn = mysql.connector.connect(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                database=config.MYSQL_DATABASE,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD
            )
            
            cursor = conn.cursor()
            sql = f"DELETE FROM {config.TABLE_RECOMMEND_RESULT} WHERE userid = %s"
            cursor.execute(sql, (user_id,))
            conn.commit()
            
            print(f"已删除用户 {user_id} 的旧推荐记录")
            
        except Exception as e:
            print(f"删除用户推荐记录时出错: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def read_personal_ratings(self, user_id):
        """从MySQL读取用户个性化评分"""
        if self.spark is None:
            self.init_spark()
        
        try:
            # 使用Spark读取MySQL数据
            df = self.spark.read \
                .format("jdbc") \
                .option("url", config.mysql_url) \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", config.TABLE_PERSONAL_RATINGS) \
                .option("user", config.MYSQL_USER) \
                .option("password", config.MYSQL_PASSWORD) \
                .load()
            
            # 过滤指定用户的评分
            df.createOrReplaceTempView("personalratings")
            result_df = self.spark.sql(f"SELECT * FROM personalratings WHERE userid = {user_id}")
            
            # 转换为字符串数组
            ratings_array = []
            if result_df.count() > 0:
                for row in result_df.collect():
                    rating_str = f"{row[0]}::{row[1]}::{row[2]}::{row[3]}"
                    ratings_array.append(rating_str)
            
            print(f"读取到 {len(ratings_array)} 条用户评分")
            return ratings_array
            
        except Exception as e:
            print(f"读取用户评分时出错: {e}")
            raise
    
    def insert_recommendations(self, recommendations, movies_dict):
        """将推荐结果插入MySQL"""
        if self.spark is None:
            self.init_spark()
        
        try:
            # 将推荐结果转换为字符串数组
            recommendations_array = []
            for r in recommendations:
                movie_name = movies_dict.get(r.product, "Unknown")
                rec_str = f"{r.user}::{r.product}::{r.rating}::{movie_name}"
                recommendations_array.append(rec_str)
            
            # 转换为RDD和DataFrame
            movie_rdd = self.spark.sparkContext.parallelize(recommendations_array).map(lambda x: x.split("::"))
            
            # 定义模式
            schema = StructType([
                StructField("userid", IntegerType(), True),
                StructField("movieid", IntegerType(), True),
                StructField("rating", FloatType(), True),
                StructField("moviename", StringType(), True)
            ])
            
            # 创建DataFrame
            row_rdd = movie_rdd.map(lambda p: (int(p[0]), int(p[1]), float(p[2]), p[3]))
            df = self.spark.createDataFrame(row_rdd, schema=schema)
            
            # 写入MySQL
            df.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", config.mysql_url) \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("dbtable", config.TABLE_RECOMMEND_RESULT) \
                .option("user", config.MYSQL_USER) \
                .option("password", config.MYSQL_PASSWORD) \
                .save()
            
            print(f"已插入 {len(recommendations_array)} 条推荐记录")
            
        except Exception as e:
            print(f"插入推荐结果时出错: {e}")
            raise