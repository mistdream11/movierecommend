#!/usr/bin/env python3
# movie_rec.py - 基于强化学习的电影推荐系统
import sys
import os
import traceback
import numpy as np
from collections import defaultdict, deque
from collections import namedtuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
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
    
    # 强化学习参数
    STATE_DIM = 20  # 状态向量维度
    EXPLORATION_RATE = 0.1  # 探索率
    LEARNING_RATE = 0.01  # 学习率
    DISCOUNT_FACTOR = 0.9  # 折扣因子
    MEMORY_SIZE = 10000  # 经验回放缓冲区大小
    
    # 数据文件
    RATINGS_FILE = "ratings.csv"
    MOVIES_FILE = "movies.csv"
    
    # Spark配置
    SPARK_APP_NAME = "MovieRecommendationRL"
    SPARK_MASTER = "local[2]"

config = Config()

# 定义强化学习相关数据结构
Rating = namedtuple('Rating', ['user', 'product', 'rating'])
Experience = namedtuple('Experience', ['state', 'action', 'reward', 'next_state', 'done'])

class ReinforcementLearningAgent:
    """强化学习智能体 - 使用上下文多臂赌博机（Contextual Bandit）方法"""
    
    def __init__(self, state_dim, n_actions, learning_rate=0.01, exploration_rate=0.1):
        self.state_dim = state_dim
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.exploration_rate = exploration_rate
        
        # 初始化Q-table或模型参数
        # 这里使用线性模型：Q(s, a) = θ_a^T * s
        self.theta = np.random.randn(n_actions, state_dim) * 0.01
        self.action_counts = np.zeros(n_actions)  # 每个动作的选择次数
        self.action_rewards = np.zeros(n_actions)  # 每个动作的总奖励
        
        # 经验回放缓冲区
        self.memory = deque(maxlen=config.MEMORY_SIZE)
        
    def get_action(self, state, available_actions, epsilon=None):
        """根据ε-greedy策略选择动作"""
        if epsilon is None:
            epsilon = self.exploration_rate
            
        # 探索：随机选择可用动作
        if np.random.random() < epsilon:
            return np.random.choice(available_actions)
        
        # 利用：选择Q值最高的动作
        q_values = np.dot(self.theta[available_actions], state)
        best_action_idx = np.argmax(q_values)
        return available_actions[best_action_idx]
    
    def update(self, state, action, reward, next_state=None, done=False):
        """更新Q值函数（使用增量更新）"""
        # 计算当前Q值
        q_value = np.dot(self.theta[action], state)
        
        if done or next_state is None:
            target = reward
        else:
            # 使用下一个状态的最大Q值作为目标的一部分
            next_q_values = np.dot(self.theta, next_state)
            target = reward + config.DISCOUNT_FACTOR * np.max(next_q_values)
        
        # 更新参数（梯度下降）
        error = target - q_value
        self.theta[action] += self.learning_rate * error * state
        
        # 更新动作统计
        self.action_counts[action] += 1
        self.action_rewards[action] += reward
        
        # 存储经验
        self.memory.append(Experience(state, action, reward, next_state, done))
        
        return error
    
    def replay_experience(self, batch_size=32):
        """经验回放 - 从记忆中采样并学习"""
        if len(self.memory) < batch_size:
            return
        
        # 随机采样一批经验
        indices = np.random.choice(len(self.memory), batch_size, replace=False)
        batch = [self.memory[i] for i in indices]
        
        for experience in batch:
            self.update(experience.state, experience.action, 
                       experience.reward, experience.next_state, experience.done)
    
    def get_action_value(self, state, action):
        """获取特定状态-动作对的Q值"""
        return np.dot(self.theta[action], state)
    
    def get_top_actions(self, state, available_actions, top_k=10):
        """获取top-k推荐动作"""
        q_values = np.dot(self.theta[available_actions], state)
        top_indices = np.argsort(q_values)[-top_k:][::-1]
        return [(available_actions[i], q_values[i]) for i in top_indices]

class MovieStateEncoder:
    """电影状态编码器 - 将用户和电影特征编码为状态向量"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.user_features = None
        self.movie_features = None
        self.user_feature_mean = None
        self.user_feature_std = None
        self.movie_feature_mean = None
        self.movie_feature_std = None
        
    def extract_features(self, ratings_df, movies_df):
        """从评分和电影数据中提取特征"""
        print("提取用户特征...")
        # 用户特征：平均评分、评分数量、评分方差
        user_stats = ratings_df.groupBy("user").agg(
            F.mean("rating").alias("avg_rating"),
            F.count("rating").alias("rating_count"),
            F.variance("rating").alias("rating_var")
        ).fillna(0)
        
        # 电影特征：平均评分、评分数量、评分时间等
        movie_stats = ratings_df.groupBy("product").agg(
            F.mean("rating").alias("avg_rating"),
            F.count("rating").alias("rating_count"),
            F.mean("timestamp").alias("avg_timestamp")
        ).fillna(0)
        
        # 合并电影元数据
        movie_features = movie_stats.join(
            movies_df.select(col("movieId").alias("product"), "genres"),
            "product", "left"
        )
        
        # 对类别特征进行编码（这里简化处理）
        self.user_features = {}
        user_feature_list = []
        for row in user_stats.collect():
            # 确保所有用户特征都是数值类型
            avg_rating = float(row.avg_rating) if row.avg_rating else 0.0
            rating_count = float(row.rating_count) if row.rating_count else 0.0
            rating_var = float(row.rating_var) if row.rating_var else 0.0
            
            feature = np.array([avg_rating, rating_count, rating_var])
            self.user_features[row.user] = feature
            user_feature_list.append(feature)
        
        self.movie_features = {}
        movie_feature_list = []
        for row in movie_features.collect():
            # 基本特征
            avg_rating = float(row.avg_rating) if row.avg_rating else 0.0
            rating_count = float(row.rating_count) if row.rating_count else 0.0
            avg_timestamp = float(row.avg_timestamp) if row.avg_timestamp else 0.0
            
            # 添加简单的类别特征
            genre_count = 0
            if row.genres:
                genres_list = row.genres.split('|')
                genre_count = float(len(genres_list))
            
            feature = np.array([avg_rating, rating_count, avg_timestamp, genre_count])
            self.movie_features[row.product] = feature
            movie_feature_list.append(feature)
        
        # 分别计算用户特征和电影特征的统计量
        if user_feature_list:
            user_feature_matrix = np.array(user_feature_list)
            self.user_feature_mean = user_feature_matrix.mean(axis=0)
            self.user_feature_std = user_feature_matrix.std(axis=0) + 1e-8  # 避免除零
        else:
            self.user_feature_mean = np.zeros(3)
            self.user_feature_std = np.ones(3)
        
        if movie_feature_list:
            movie_feature_matrix = np.array(movie_feature_list)
            self.movie_feature_mean = movie_feature_matrix.mean(axis=0)
            self.movie_feature_std = movie_feature_matrix.std(axis=0) + 1e-8
        else:
            self.movie_feature_mean = np.zeros(4)
            self.movie_feature_std = np.ones(4)
        
        print(f"特征提取完成，用户特征数: {len(self.user_features)}, 电影特征数: {len(self.movie_features)}")
        print(f"用户特征维度: {len(self.user_feature_mean)}, 电影特征维度: {len(self.movie_feature_mean)}")
    
    def encode_state(self, user_id, movie_id=None):
        """编码状态向量：用户特征 + 最后交互的电影特征"""
        # 获取用户特征
        user_feat = self.user_features.get(user_id)
        if user_feat is None:
            # 新用户，使用平均特征
            user_feat = np.array([3.0, 0, 0])
        
        # 获取电影特征（如果提供）
        if movie_id and movie_id in self.movie_features:
            movie_feat = self.movie_features[movie_id]
        else:
            # 如果没有特定电影，使用平均电影特征
            movie_feat = np.array([3.0, 0, 0, 0])
        
        # 分别标准化用户特征和电影特征
        user_feat_standardized = (user_feat - self.user_feature_mean) / self.user_feature_std
        movie_feat_standardized = (movie_feat - self.movie_feature_mean) / self.movie_feature_std
        
        # 合并特征
        state = np.concatenate([user_feat_standardized, movie_feat_standardized])
        
        # 如果维度不够，用零填充
        if len(state) < config.STATE_DIM:
            state = np.pad(state, (0, config.STATE_DIM - len(state)))
        elif len(state) > config.STATE_DIM:
            state = state[:config.STATE_DIM]
        
        return state

    def get_available_actions(self, user_id, rated_movies, all_movies, max_actions=1000):
        """获取可用动作（未评分的电影）"""
        # 排除已评分的电影
        available = [m for m in all_movies if m not in rated_movies]
        
        # 限制动作空间大小以提高效率
        if len(available) > max_actions:
            # 可以基于流行度或随机选择来限制
            np.random.shuffle(available)
            available = available[:max_actions]
        
        return available

class DatabaseOperations:
    """数据库操作类"""
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
            INSERT INTO {config.TABLE_RECOMMEND_RESULT} (userid, movieid, rating, moviename) 
            VALUES (%s, %s, %s, %s)
            """
            
            for r in recommendations:
                movie_name = movies_dict.get(r.product, "Unknown")
                
                # 确保评分值在0-5范围内且为整数
                rating_value = float(r.rating)
                # 限制在0-5范围内
                rating_value = max(0.0, min(5.0, rating_value))
                # 四舍五入为整数
                rating_value = round(rating_value)
                
                cursor.execute(insert_sql, (r.user, r.product, rating_value, movie_name))
                print(f"插入推荐: 用户 {r.user}, 电影 {r.product} ({movie_name}), 评分 {rating_value}")
            
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

class MovieRecommendationRL:
    def __init__(self):
        self.spark = None
        self.db_ops = DatabaseOperations()
        self.agent = None
        self.state_encoder = None
        
    def init_spark(self):
        """初始化Spark会话"""
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
    
    def load_movielens_data(self, data_dir):
        """加载MovieLens CSV数据"""
        ratings_path = f"{data_dir}/{config.RATINGS_FILE}"
        movies_path = f"{data_dir}/{config.MOVIES_FILE}"
        
        if not os.path.exists(ratings_path):
            print(f"错误: 评分文件不存在: {ratings_path}")
            raise FileNotFoundError(f"评分文件不存在: {ratings_path}")
        
        if not os.path.exists(movies_path):
            print(f"错误: 电影文件不存在: {movies_path}")
            raise FileNotFoundError(f"电影文件不存在: {movies_path}")
        
        print(f"加载评分数据: {ratings_path}")
        print(f"加载电影数据: {movies_path}")
        
        try:
            # 加载评分数据
            ratings_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"file://{os.path.abspath(ratings_path)}")
            
            ratings_df = ratings_df.select(
                col("userId").alias("user"),
                col("movieId").alias("product"),
                col("rating"),
                col("timestamp")
            )
            
            # 加载电影数据
            movies_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"file://{os.path.abspath(movies_path)}")
            
            # 创建电影ID到电影名称的映射
            movies_dict = {}
            for row in movies_df.collect():
                movies_dict[row.movieId] = row.title
            
            return ratings_df, movies_df, movies_dict
            
        except Exception as e:
            print(f"加载数据时出错: {e}")
            # 尝试不使用file://前缀
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
            
            movies_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(movies_path)
            
            movies_dict = {}
            for row in movies_df.collect():
                movies_dict[row.movieId] = row.title
            
            return ratings_df, movies_df, movies_dict
    
    def train_agent(self, ratings_df, movies_df):
        """训练强化学习智能体"""
        print("训练强化学习智能体...")
        
        # 初始化状态编码器
        self.state_encoder = MovieStateEncoder(self.spark)
        self.state_encoder.extract_features(ratings_df, movies_df)
        
        # 获取所有电影ID
        all_movie_ids = list(self.state_encoder.movie_features.keys())
        n_actions = len(all_movie_ids)
        
        # 初始化智能体
        self.agent = ReinforcementLearningAgent(
            state_dim=config.STATE_DIM,
            n_actions=n_actions,
            learning_rate=config.LEARNING_RATE,
            exploration_rate=config.EXPLORATION_RATE
        )
        
        # 创建电影ID到动作索引的映射
        self.movie_to_action = {movie_id: idx for idx, movie_id in enumerate(all_movie_ids)}
        self.action_to_movie = {idx: movie_id for idx, movie_id in enumerate(all_movie_ids)}
        
        # 使用评分数据训练智能体（离线训练）
        print("使用离线评分数据进行训练...")
        ratings_data = ratings_df.collect()
        
        # 按用户分组
        user_ratings = defaultdict(list)
        for row in ratings_data:
            user_ratings[row.user].append((row.product, row.rating))
        
        # 训练循环
        training_episodes = min(1000, len(user_ratings))  # 限制训练轮数
        
        for episode, (user_id, ratings) in enumerate(list(user_ratings.items())[:training_episodes]):
            if episode % 100 == 0:
                print(f"训练进度: {episode}/{training_episodes}")
            
            # 按时间排序（如果有时间戳）
            ratings.sort(key=lambda x: x[1] if len(x) > 2 else 0)
            
            # 初始化状态
            state = self.state_encoder.encode_state(user_id)
            
            for i, (movie_id, rating) in enumerate(ratings):
                # 将电影ID转换为动作索引
                if movie_id not in self.movie_to_action:
                    continue
                    
                action = self.movie_to_action[movie_id]
                
                # 计算奖励（评分归一化到0-1之间）
                reward = rating / 5.0
                
                # 如果有下一个评分，将其作为下一个状态
                next_state = None
                done = (i == len(ratings) - 1)
                
                if not done and i + 1 < len(ratings):
                    next_movie_id = ratings[i + 1][0]
                    next_state = self.state_encoder.encode_state(user_id, next_movie_id)
                
                # 更新智能体
                self.agent.update(state, action, reward, next_state, done)
                
                # 更新状态
                if next_state is not None:
                    state = next_state
                else:
                    state = self.state_encoder.encode_state(user_id, movie_id)
        
        print(f"智能体训练完成，经验回放缓冲区大小: {len(self.agent.memory)}")
        
        # 执行经验回放
        if len(self.agent.memory) > 0:
            print("执行经验回放...")
            for _ in range(10):  # 回放10轮
                self.agent.replay_experience(batch_size=min(64, len(self.agent.memory)))
    
    def generate_recommendations(self, user_id, personal_ratings, movies_dict, top_k=10):
        """为特定用户生成推荐"""
        print(f"为用户 {user_id} 生成推荐...")
        
        # 获取用户已评分的电影
        my_rated_movie_ids = set([r.product for r in personal_ratings])
        
        # 获取可用动作（未评分的电影）
        available_movies = self.state_encoder.get_available_actions(
            user_id, my_rated_movie_ids, list(self.movie_to_action.keys())
        )
        
        # 转换为动作索引
        available_actions = [self.movie_to_action[movie_id] for movie_id in available_movies 
                           if movie_id in self.movie_to_action]
        
        if not available_actions:
            print("没有可用的候选电影")
            return []
        
        # 编码当前状态
        current_state = self.state_encoder.encode_state(user_id)
        
        # 获取top-k推荐动作
        top_actions = self.agent.get_top_actions(current_state, available_actions, top_k=top_k)
        
        # 转换为推荐结果
        recommendations = []
        for action_idx, q_value in top_actions:
            movie_id = self.action_to_movie[action_idx]
            
            # 计算预测评分（Q值映射到1-5分，并确保为整数）
            # 使用sigmoid函数将Q值限制在合理范围内
            predicted_rating = 1 / (1 + np.exp(-q_value))  # sigmoid函数，值在0-1之间
            predicted_rating = 1 + predicted_rating * 4  # 映射到1-5范围
            predicted_rating = round(predicted_rating)  # 四舍五入为整数
            
            # 确保评分在1-5范围内
            predicted_rating = max(1, min(5, predicted_rating))
            
            recommendation = Rating(
                user=user_id,
                product=movie_id,
                rating=float(predicted_rating)  # 转换为浮点数但实际是整数
            )
            recommendations.append(recommendation)
        
        return recommendations
    
    def run(self, data_dir, user_id):
        """运行强化学习推荐系统"""
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
            ratings_df, movies_df, movies_dict = self.load_movielens_data(data_dir)
            
            # 5. 训练或加载智能体
            if self.agent is None:
                self.train_agent(ratings_df, movies_df)
            
            # 6. 生成推荐
            recommendations = self.generate_recommendations(user_id, my_ratings, movies_dict, top_k=10)
            
            if not recommendations:
                print("无法生成推荐，使用基于流行度的后备策略")
                # 后备策略：选择评分次数最多的电影
                movie_counts = ratings_df.groupBy("product").count().orderBy(F.col("count").desc())
                top_movies = movie_counts.limit(10).collect()
                
                recommendations = []
                for row in top_movies:
                    if row.product not in [r.product for r in my_ratings]:
                        recommendation = Rating(
                            user=user_id,
                            product=row.product,
                            rating=4.0  # 默认评分
                        )
                        recommendations.append(recommendation)
            
            # 7. 保存推荐结果到数据库
            print("保存推荐结果到数据库...")
            self.db_ops.insert_recommendations(recommendations, movies_dict)
            
            # 8. 打印推荐结果
            print("\n=== 推荐结果 ===")
            print("用户ID | 电影ID | 预测评分 | 电影名称")
            print("-" * 60)
            for i, rec in enumerate(recommendations, 1):
                movie_name = movies_dict.get(rec.product, "Unknown")
                print(f"{rec.user} | {rec.product} | {rec.rating:.0f} | {movie_name}")
            
            print("\n推荐完成！")
            
            # 9. 在线学习：使用用户历史评分更新模型
            print("使用用户历史评分进行在线学习...")
            current_state = self.state_encoder.encode_state(user_id)
            
            for rating in my_ratings:
                if rating.product in self.movie_to_action:
                    action = self.movie_to_action[rating.product]
                    reward = rating.rating / 5.0
                    self.agent.update(current_state, action, reward)
            
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
            print("用法: python movie_rec.py <movieLens数据目录> <用户ID>")
            print("示例: python movie_rec.py /home/zsh/ml-latest-small 1")
            sys.exit(1)
        
        data_dir = sys.argv[1]
        user_id = int(sys.argv[2])
        
        self.run(data_dir, user_id)

if __name__ == "__main__":
    recommender = MovieRecommendationRL()
    recommender.main()