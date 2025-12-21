import pandas as pd
import pymysql
from pymysql import Error
import numpy as np
from datetime import datetime
import re

def extract_year_from_title(title):
    """从电影标题中提取年份"""
    match = re.search(r'\((\d{4})\)', title)
    if match:
        return int(match.group(1))
    return None

def clean_movie_title(title):
    """清理电影标题，移除年份信息"""
    # 移除标题末尾的年份信息，如 "Toy Story (1995)" -> "Toy Story"
    cleaned_title = re.sub(r'\s*\(\d{4}\)$', '', title)
    return cleaned_title.strip()

def create_movieinfo_table(connection):
    """创建movieinfo表（如果不存在）"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS movieinfo (
        movieid INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        moviename VARCHAR(255) NOT NULL,
        picture VARCHAR(500),
        movieurl VARCHAR(500),
        genre VARCHAR(100),
        director VARCHAR(100),
        actors TEXT,
        year INT,
        rating DECIMAL(3,1),
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
        connection.commit()
        print("✓ movieinfo表已创建或已存在")
    except Error as e:
        print(f"✗ 创建表时出错: {e}")
        raise

def load_and_process_movies(movies_file):
    """加载和处理movies.csv文件"""
    print(f"正在加载 {movies_file}...")
    
    # 读取movies.csv文件
    movies_df = pd.read_csv(movies_file)
    
    # 提取年份并清理标题
    movies_df['year'] = movies_df['title'].apply(extract_year_from_title)
    movies_df['moviename'] = movies_df['title'].apply(clean_movie_title)
    
    # 重命名列以匹配数据库表结构
    movies_df = movies_df.rename(columns={
        'movieId': 'movieid',
        'genres': 'genre'
    })
    
    # 只保留需要的列
    movies_df = movies_df[['movieid', 'moviename', 'genre', 'year']]
    
    print(f"✓ 已加载 {len(movies_df)} 部电影")
    return movies_df

def load_and_process_ratings(ratings_file):
    """加载和处理ratings.csv文件，计算每部电影的平均评分"""
    print(f"正在加载 {ratings_file}...")
    
    # 读取ratings.csv文件
    ratings_df = pd.read_csv(ratings_file)
    
    # 计算每部电影的平均评分
    movie_ratings = ratings_df.groupby('movieId')['rating'].agg(['mean', 'count']).reset_index()
    movie_ratings = movie_ratings.rename(columns={
        'movieId': 'movieid',
        'mean': 'rating'
    })
    
    print(f"✓ 已处理 {len(ratings_df)} 条评分记录")
    return movie_ratings[['movieid', 'rating']]

def merge_movie_data(movies_df, ratings_df):
    """合并电影基本信息和评分数据"""
    print("正在合并电影数据和评分数据...")
    
    # 合并数据
    merged_df = pd.merge(movies_df, ratings_df, on='movieid', how='left')
    
    # 将缺失的评分设为NULL
    merged_df['rating'] = merged_df['rating'].where(pd.notnull(merged_df['rating']), None)
    
    print(f"✓ 合并完成，总共 {len(merged_df)} 条记录")
    return merged_df

def insert_data_to_mysql(connection, df):
    """将数据插入到MySQL数据库"""
    insert_query = """
    INSERT INTO movieinfo 
    (movieid, moviename, genre, year, rating) 
    VALUES (%s, %s, %s, %s, %s)
    """
    
    # 准备数据
    data = []
    for _, row in df.iterrows():
        # 处理可能为NaN的值
        year = int(row['year']) if pd.notnull(row['year']) else None
        rating = float(row['rating']) if pd.notnull(row['rating']) else None
        
        data.append((
            int(row['movieid']),
            str(row['moviename']),
            str(row['genre']) if pd.notnull(row['genre']) else None,
            year,
            rating
        ))
    
    try:
        with connection.cursor() as cursor:
            # 批量插入数据
            cursor.executemany(insert_query, data)
        connection.commit()
        print(f"✓ 成功插入 {len(data)} 条记录到数据库")
        
        # 显示插入的记录数统计
        cursor.execute("SELECT COUNT(*) FROM movieinfo")
        total_records = cursor.fetchone()[0]
        print(f"✓ 数据库当前总记录数: {total_records}")
        
    except Error as e:
        print(f"✗ 插入数据时出错: {e}")
        # 如果是重复键错误，可以选择更新现有记录
        if "Duplicate entry" in str(e):
            print("尝试更新现有记录...")
            update_existing_records(connection, data)
        else:
            raise

def update_existing_records(connection, data):
    """更新数据库中已存在的记录"""
    update_query = """
    UPDATE movieinfo 
    SET moviename = %s, genre = %s, year = %s, rating = %s
    WHERE movieid = %s
    """
    
    # 重新排列数据顺序以匹配UPDATE语句
    update_data = [(moviename, genre, year, rating, movieid) 
                   for movieid, moviename, genre, year, rating in data]
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(update_query, update_data)
        connection.commit()
        print(f"✓ 成功更新 {len(update_data)} 条记录")
    except Error as e:
        print(f"✗ 更新记录时出错: {e}")

def main():
    # 数据库连接配置
    db_config = {
        'host': 'localhost',
        'user': 'root',           # 替换为你的MySQL用户名
        'password': '123456',   # 替换为你的MySQL密码
        'database': 'movierecommend',  # 替换为你的数据库名
        'charset': 'utf8mb4'
    }
    
    # CSV文件路径
    movies_file = '/home/zsh/ml-latest-small/movies.csv'
    ratings_file = '/home/zsh/ml-latest-small/ratings.csv'
    
    try:
        # 连接到MySQL数据库
        print("正在连接到MySQL数据库...")
        connection = pymysql.connect(**db_config)
        print("✓ 数据库连接成功")
        
        # 创建表
        create_movieinfo_table(connection)
        
        # 加载和处理数据
        movies_df = load_and_process_movies(movies_file)
        ratings_df = load_and_process_ratings(ratings_file)
        
        # 合并数据
        merged_df = merge_movie_data(movies_df, ratings_df)
        
        # 显示数据示例
        print("\n数据示例（前5行）:")
        print(merged_df.head())
        
        # 插入数据到数据库
        print("\n正在插入数据到数据库...")
        insert_data_to_mysql(connection, merged_df)
        
        # 验证数据插入
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT movieid, moviename, year, rating 
                FROM movieinfo 
                WHERE rating IS NOT NULL 
                ORDER BY rating DESC 
                LIMIT 5
            """)
            top_movies = cursor.fetchall()
            
            print("\n数据库中最受好评的电影（前5名）:")
            for movie in top_movies:
                print(f"ID: {movie[0]}, 电影: {movie[1]}, 年份: {movie[2]}, 评分: {movie[3]:.1f}")
                
    except FileNotFoundError as e:
        print(f"✗ 文件未找到: {e}")
        print("请确保 movies.csv 和 ratings.csv 文件在当前目录下")
    except Error as e:
        print(f"✗ 数据库错误: {e}")
    except Exception as e:
        print(f"✗ 发生错误: {e}")
    finally:
        # 关闭数据库连接
        if 'connection' in locals() and connection.open:
            connection.close()
            print("\n数据库连接已关闭")

if __name__ == "__main__":
    main()