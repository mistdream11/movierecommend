#!/usr/bin/env python3
# movie_als.py
import sys
import os
import traceback
from collections import namedtuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置信息
class Config:
    # MySQL配置
    MYSQL_HOST = "localhost"
    MYSQL_PORT = 3306
    MYSQL_DATABASE = "movierecommend"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "123456"
    
    # 数据表名
    TABLE_PERSONAL_RATINGS = "personalratings"
    TABLE_RECOMMEND_RESULT = "recommendresult"
    
    # 模型参数
    RANKS = [8, 12]
    LAMBDAS = [0.1, 10.0]
    NUM_ITERS = [10, 20]
    NUM_PARTITIONS = 4
    
    # 数据文件
    RATINGS_FILE = "ratings.csv"
    MOVIES_FILE = "movies.csv"
    
    # Spark配置
    SPARK_APP_NAME = "MovieLensALS"
    SPARK_MASTER = "local[2]"

config = Config()

# 定义Rating类
Rating = namedtuple('Rating', ['user', 'product', 'rating'])

class DatabaseOperations:
    """数据库操作类 - 使用纯Python连接MySQL"""
    def __init__(self):
        try:
            import pymysql
            self.pymysql = pymysql
        except ImportError:
            print("警告: pymysql未安装，使用pip install pymysql安装")
            raise
        
        self.mysql_config = {
            'host': config.MYSQL_HOST,
            'port': config.MYSQL_PORT,
            'user': config.MYSQL_USER,
            'password': config.MYSQL_PASSWORD,
            'database': config.MYSQL_DATABASE,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
    
    def get_connection(self):
        """获取MySQL连接"""
        return self.pymysql.connect(**self.mysql_config)
    
    def delete_user_recommendations(self, user_id):
        """删除用户的旧推荐结果"""
        try:
            conn = self.get_connection()
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
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            sql = f"SELECT userid, movieid, rating, timestamp FROM {config.TABLE_PERSONAL_RATINGS} WHERE userid = %s"
            cursor.execute(sql, (user_id,))
            rows = cursor.fetchall()
            
            # 转换为字符串数组
            ratings_array = []
            for row in rows:
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
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 先删除该用户的所有推荐结果
            delete_sql = f"DELETE FROM {config.TABLE_RECOMMEND_RESULT} WHERE userid = %s"
            cursor.execute(delete_sql, (recommendations[0].user,))
            
            # 插入新的推荐结果
            insert_sql = f"""
            INSERT INTO {config.TABLE_RECOMMEND_RESULT} (userid, movieid, rating) 
            VALUES (%s, %s, %s)
            """
            
            for r in recommendations:
                movie_name = movies_dict.get(r.product, "Unknown")
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

class MovieLensALS:
    def __init__(self):
        self.spark = None
        self.db_ops = DatabaseOperations()
    
    def init_spark(self):
        """初始化Spark会话 - 修复HDFS连接问题"""
        self.spark = SparkSession.builder \
            .appName(config.SPARK_APP_NAME) \
            .master(config.SPARK_MASTER) \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .getOrCreate()
        
        # 设置日志级别
        self.spark.sparkContext.setLogLevel("ERROR")
        return self.spark
    
    def load_ratings(self, lines):
        """加载用户评分"""
        ratings = []
        for line in lines:
            fields = line.split("::")
            if len(fields) >= 3:
                rating = Rating(
                    user=int(fields[0]),
                    product=int(fields[1]),
                    rating=float(fields[2])
                )
                if rating.rating > 0.0:
                    ratings.append(rating)
        
        if not ratings:
            print("警告: 没有提供用户评分，将使用通用推荐")
            return []
        
        return ratings
    
    def compute_rmse(self, model, df, n):
        """计算RMSE"""
        predictions = model.transform(df.select("user", "product"))
        
        # 将预测值和真实值连接
        predictions_and_ratings = predictions.select(
            "user", "product", "prediction"
        ).join(
            df.select("user", "product", "rating"),
            ["user", "product"]
        )
        
        # 计算RMSE
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions_and_ratings)
        return rmse
    
    def load_movielens_data(self, data_dir):
        """加载MovieLens CSV数据 - 修复路径问题"""
        # 检查文件是否存在
        ratings_path = f"{data_dir}/{config.RATINGS_FILE}"
        movies_path = f"{data_dir}/{config.MOVIES_FILE}"
        
        if not os.path.exists(ratings_path):
            print(f"错误: 评分文件不存在: {ratings_path}")
            print("请确保文件路径正确，或下载MovieLens数据集")
            raise FileNotFoundError(f"评分文件不存在: {ratings_path}")
        
        if not os.path.exists(movies_path):
            print(f"错误: 电影文件不存在: {movies_path}")
            raise FileNotFoundError(f"电影文件不存在: {movies_path}")
        
        print(f"加载评分数据: {ratings_path}")
        print(f"加载电影数据: {movies_path}")
        
        # 使用绝对路径并添加 file:// 前缀
        ratings_path_with_prefix = f"file://{os.path.abspath(ratings_path)}"
        movies_path_with_prefix = f"file://{os.path.abspath(movies_path)}"
        
        try:
            # 加载评分数据
            ratings_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(ratings_path_with_prefix)
            
            # 重命名列
            ratings_df = ratings_df.select(
                col("userId").alias("user"),
                col("movieId").alias("product"),
                col("rating"),
                col("timestamp")
            )
            
            # 添加key列（timestamp % 10）
            ratings_df = ratings_df.withColumn("key", col("timestamp") % 10)
            
            # 加载电影数据
            movies_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(movies_path_with_prefix)
            
            # 创建电影ID到电影名称的映射
            movies_dict = {}
            for row in movies_df.collect():
                movies_dict[row.movieId] = row.title
            
            return ratings_df, movies_dict
            
        except Exception as e:
            print(f"加载数据时出错: {e}")
            # 尝试不使用file://前缀
            print("尝试不使用file://前缀...")
            ratings_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(ratings_path)
            
            ratings_df = ratings_df.select(
                col("userId").alias("user"),
                col("movieId").alias("product"),
                col("rating"),
                col("timestamp")
            )
            
            ratings_df = ratings_df.withColumn("key", col("timestamp") % 10)
            
            movies_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(movies_path)
            
            movies_dict = {}
            for row in movies_df.collect():
                movies_dict[row.movieId] = row.title
            
            return ratings_df, movies_dict
    
    def run(self, data_dir, user_id):
        """运行推荐系统"""
        try:
            # 1. 初始化Spark
            self.init_spark()
            
            # 2. 删除旧推荐结果
            print(f"删除用户 {user_id} 的旧推荐记录...")
            self.db_ops.delete_user_recommendations(user_id)
            
            # 3. 读取用户评分
            print("读取用户评分数据...")
            personal_ratings_lines = self.db_ops.read_personal_ratings(user_id)
            my_ratings = self.load_ratings(personal_ratings_lines)
            
            # 4. 加载MovieLens数据
            print("加载MovieLens数据集...")
            ratings_df, movies_dict = self.load_movielens_data(data_dir)
            
            # 5. 准备数据集
            print("准备训练、验证和测试集...")
            
            # 将用户评分添加到训练集
            if my_ratings:
                my_ratings_df = self.spark.createDataFrame(my_ratings)
                training_df = ratings_df.filter(col("key") < 6) \
                    .select("user", "product", "rating") \
                    .union(my_ratings_df) \
                    .repartition(config.NUM_PARTITIONS)
            else:
                training_df = ratings_df.filter(col("key") < 6) \
                    .select("user", "product", "rating") \
                    .repartition(config.NUM_PARTITIONS)
            
            validation_df = ratings_df.filter((col("key") >= 6) & (col("key") < 8)) \
                .select("user", "product", "rating") \
                .repartition(config.NUM_PARTITIONS)
            
            test_df = ratings_df.filter(col("key") >= 8) \
                .select("user", "product", "rating")
            
            num_training = training_df.count()
            num_validation = validation_df.count()
            num_test = test_df.count()
            
            print(f"训练集大小: {num_training}")
            print(f"验证集大小: {num_validation}")
            print(f"测试集大小: {num_test}")
            
            # 6. 训练模型（简化版，只使用一组参数）
            print("开始训练ALS模型...")
            
            als = ALS(
                maxIter=10,
                rank=10,
                regParam=0.1,
                userCol="user",
                itemCol="product",
                ratingCol="rating",
                coldStartStrategy="drop"
            )
            
            model = als.fit(training_df)
            
            # 7. 生成推荐
            print("生成电影推荐...")
            my_rated_movie_ids = set([r.product for r in my_ratings])
            
            # 获取所有电影的ID
            all_movie_ids = list(movies_dict.keys())
            
            # 创建候选集（排除已评分的电影）
            candidate_movie_ids = [mid for mid in all_movie_ids 
                                 if mid not in my_rated_movie_ids]
            
            # 如果候选集太大，只取前1000个
            if len(candidate_movie_ids) > 1000:
                candidate_movie_ids = candidate_movie_ids[:1000]
            
            candidates_df = self.spark.createDataFrame(
                [(user_id, mid, 0.0) for mid in candidate_movie_ids],
                schema=["user", "product", "rating"]
            ).select("user", "product")
            
            # 预测评分
            recommendations_df = model.transform(candidates_df)
            
            # 获取前10个推荐
            recommendations = recommendations_df.select(
                "user", "product", "prediction"
            ).orderBy(F.col("prediction").desc()) \
             .limit(10) \
             .rdd.map(
                lambda row: Rating(
                    user=row.user,
                    product=row.product,
                    rating=float(row.prediction)
                )
            ).collect()
            
            # 8. 保存推荐结果到数据库
            print("保存推荐结果到数据库...")
            self.db_ops.insert_recommendations(recommendations, movies_dict)
            
            # 9. 打印推荐结果
            print("\n=== 推荐结果 ===")
            print("用户ID | 电影ID | 预测评分 | 电影名称")
            print("-" * 60)
            for i, rec in enumerate(recommendations, 1):
                movie_name = movies_dict.get(rec.product, "Unknown")
                print(f"{rec.user} | {rec.product} | {rec.rating:.4f} | {movie_name}")
            
            print("\n推荐完成！")
            
        except Exception as e:
            print(f"运行出错: {e}")
            traceback.print_exc()
            raise
        finally:
            if self.spark:
                self.spark.stop()
    
    def main(self):
        """主函数"""
        if len(sys.argv) != 3:
            print("用法: python movie_als.py <movieLens数据目录> <用户ID>")
            print("示例: python movie_als.py /home/zsh/ml-latest-small 1")
            sys.exit(1)
        
        data_dir = sys.argv[1]
        user_id = int(sys.argv[2])
        
        self.run(data_dir, user_id)

if __name__ == "__main__":
    recommender = MovieLensALS()
    recommender.main()