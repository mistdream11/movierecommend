# db_operations.py - 使用pymysql直接连接MySQL
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from config import config

class DatabaseOperations:
    def __init__(self):
        self.spark = None
        self.mysql_config = {
            'host': config.MYSQL_HOST,
            'port': config.MYSQL_PORT,
            'user': config.MYSQL_USER,
            'password': config.MYSQL_PASSWORD,
            'database': config.MYSQL_DATABASE,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
    
    def init_spark(self):
        """初始化Spark会话"""
        self.spark = SparkSession.builder \
            .appName("DBOperations") \
            .master(config.SPARK_MASTER) \
            .config("spark.jars", "/usr/local/spark/jars/mysql-connector-java-8.0.33.jar") \
            .getOrCreate()
        return self.spark
    
    def get_mysql_connection(self):
        """获取MySQL连接"""
        return pymysql.connect(**self.mysql_config)
    
    def delete_user_recommendations(self, user_id):
        """删除用户的旧推荐结果"""
        try:
            conn = self.get_mysql_connection()
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
        """从MySQL读取用户个性化评分 - 使用pymysql"""
        try:
            conn = self.get_mysql_connection()
            cursor = conn.cursor()
            
            sql = f"SELECT * FROM {config.TABLE_PERSONAL_RATINGS} WHERE userid = %s"
            cursor.execute(sql, (user_id,))
            rows = cursor.fetchall()
            
            # 转换为字符串数组（保持与原Scala代码兼容）
            ratings_array = []
            for row in rows:
                # 假设表结构为：userid, movieid, rating, timestamp
                rating_str = f"{row['userid']}::{row['movieid']}::{row['rating']}::{row['timestamp']}"
                ratings_array.append(rating_str)
            
            print(f"读取到 {len(ratings_array)} 条用户评分")
            return ratings_array
            
        except Exception as e:
            print(f"读取用户评分时出错: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def insert_recommendations(self, recommendations, movies_dict):
        """将推荐结果插入MySQL"""
        try:
            conn = self.get_mysql_connection()
            cursor = conn.cursor()
            
            # 先删除该用户的所有推荐结果
            delete_sql = f"DELETE FROM {config.TABLE_RECOMMEND_RESULT} WHERE userid = %s"
            cursor.execute(delete_sql, (recommendations[0].user,))
            
            # 插入新的推荐结果 - 现在有4个字段：userid, movieid, rating, moviename
            insert_sql = f"""
            INSERT INTO {config.TABLE_RECOMMEND_RESULT} (userid, movieid, rating, moviename) 
            VALUES (%s, %s, %s, %s)
            """
            
            for r in recommendations:
                movie_name = movies_dict.get(r.product, "Unknown")
                # 现在传递4个参数：user, product, rating, movie_name
                cursor.execute(insert_sql, (r.user, r.product, r.rating, movie_name))
            
            conn.commit()
            print(f"已插入 {len(recommendations)} 条推荐记录")
            
        except Exception as e:
            print(f"插入推荐结果时出错: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()