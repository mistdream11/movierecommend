import sys
import math
from typing import List, Tuple, Optional, Dict
from collections import namedtuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F

from config import config
from db_operations import DatabaseOperations

Rating = namedtuple('Rating', ['user', 'product', 'rating'])

class MovieLensALS:
    def __init__(self):
        self.spark = None
        self.db_ops = DatabaseOperations()
    
    def init_spark(self):
        """初始化Spark会话"""
        self.spark = SparkSession.builder \
            .appName(config.SPARK_APP_NAME) \
            .master(config.SPARK_MASTER) \
            .getOrCreate()
        
        # 设置日志级别
        self.spark.sparkContext.setLogLevel("ERROR")
        return self.spark
    
    def load_ratings(self, lines: List[str]) -> List[Rating]:
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
            raise ValueError("No ratings provided.")
        
        return ratings
    
    def compute_rmse(self, model, df, n: int) -> float:
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
    
    def train_and_evaluate(self, training_df, validation_df, test_df, 
                          num_training, num_validation, num_test):
        """训练和评估模型"""
        best_model = None
        best_validation_rmse = float('inf')
        best_rank = 0
        best_lambda = 0.0
        best_num_iter = 0
        
        for rank in config.RANKS:
            for lambda_ in config.LAMBDAS:
                for num_iter in config.NUM_ITERS:
                    print(f"正在训练模型 - rank={rank}, lambda={lambda_}, iterations={num_iter}")
                    
                    als = ALS(
                        maxIter=num_iter,
                        rank=rank,
                        regParam=lambda_,
                        userCol="user",
                        itemCol="product",
                        ratingCol="rating",
                        coldStartStrategy="drop"
                    )
                    
                    model = als.fit(training_df)
                    
                    # 计算验证集RMSE
                    validation_rmse = self.compute_rmse(model, validation_df, num_validation)
                    print(f"验证集RMSE: {validation_rmse}")
                    
                    if validation_rmse < best_validation_rmse:
                        best_model = model
                        best_validation_rmse = validation_rmse
                        best_rank = rank
                        best_lambda = lambda_
                        best_num_iter = num_iter
        
        # 用最佳模型测试
        test_rmse = self.compute_rmse(best_model, test_df, num_test)
        
        # 计算基准RMSE
        mean_rating = training_df.union(validation_df).select(
            F.avg("rating")
        ).collect()[0][0]
        
        baseline_rmse = math.sqrt(
            test_df.select(
                F.pow(F.col("rating") - mean_rating, 2)
            ).agg(F.avg(F.col("pow(rating - mean_rating, 2)"))).collect()[0][0]
        )
        
        improvement = (baseline_rmse - test_rmse) / baseline_rmse * 100
        
        print(f"\n=== 模型评估结果 ===")
        print(f"最佳参数: rank={best_rank}, lambda={best_lambda}, iterations={best_num_iter}")
        print(f"验证集最佳RMSE: {best_validation_rmse}")
        print(f"测试集RMSE: {test_rmse}")
        print(f"基准RMSE: {baseline_rmse}")
        print(f"改进百分比: {improvement:.2f}%")
        
        return best_model
    
    def load_movielens_data(self, data_dir):
        """加载MovieLens CSV数据"""
        # 加载评分数据
        ratings_path = f"{data_dir}/{config.RATINGS_FILE}"
        print(f"加载评分数据: {ratings_path}")
        
        ratings_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ratings_path)
        
        # 重命名列以匹配我们的Rating类
        ratings_df = ratings_df.select(
            col("userId").alias("user"),
            col("movieId").alias("product"),
            col("rating"),
            col("timestamp")
        )
        
        # 添加key列（timestamp % 10）
        ratings_df = ratings_df.withColumn("key", col("timestamp") % 10)
        
        # 加载电影数据
        movies_path = f"{data_dir}/{config.MOVIES_FILE}"
        print(f"加载电影数据: {movies_path}")
        
        movies_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(movies_path)
        
        # 创建电影ID到电影名称的映射
        movies_dict = {}
        for row in movies_df.collect():
            movies_dict[row.movieId] = row.title
        
        return ratings_df, movies_dict
    
    def run(self, data_dir: str, user_id: int):
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
            my_ratings_df = self.spark.createDataFrame(my_ratings)
            
            # 划分数据集（基于key列）
            training_df = ratings_df.filter(col("key") < 6) \
                .select("user", "product", "rating") \
                .union(my_ratings_df) \
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
            
            # 6. 训练模型
            print("开始训练ALS模型...")
            best_model = self.train_and_evaluate(
                training_df, validation_df, test_df,
                num_training, num_validation, num_test
            )
            
            # 7. 生成推荐
            print("生成电影推荐...")
            my_rated_movie_ids = set([r.product for r in my_ratings])
            
            # 获取所有电影的ID（从movies_dict）
            all_movie_ids = list(movies_dict.keys())
            
            # 创建候选集（排除已评分的电影）
            candidate_movie_ids = [mid for mid in all_movie_ids 
                                 if mid not in my_rated_movie_ids]
            
            candidates_df = self.spark.createDataFrame(
                [(user_id, mid, 0.0) for mid in candidate_movie_ids],
                schema=["user", "product", "rating"]
            ).select("user", "product")
            
            # 预测评分
            recommendations_df = best_model.transform(candidates_df)
            
            # 获取前10个推荐
            recommendations = recommendations_df.select(
                "user", "product", "prediction"
            ).rdd.map(
                lambda row: Rating(
                    user=row.user,
                    product=row.product,
                    rating=float(row.prediction)
                )
            ).sortBy(lambda x: -x.rating) \
             .take(10)
            
            # 8. 保存推荐结果到数据库
            print("保存推荐结果到数据库...")
            self.db_ops.insert_recommendations(recommendations, movies_dict)
            
            # 9. 打印推荐结果
            print("\n=== 为您推荐的电影 ===")
            print("用户ID | 电影ID | 预测评分 | 电影名称")
            print("-" * 60)
            for i, r in enumerate(recommendations, 1):
                movie_name = movies_dict.get(r.product, "Unknown")
                print(f"{r.user} | {r.product} | {r.rating:.4f} | {movie_name}")
            
            print("\n推荐完成！")
            
        except Exception as e:
            print(f"运行出错: {e}")
            import traceback
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