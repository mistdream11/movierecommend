#!/usr/bin/env python3
"""
豆瓣电影爬虫 - 生成CSV文件版本
输出文件：
  movies.csv - movieId,title,genres(类别，多个类别由|隔开）
  links.csv - movieId,img(电影的缩略图链接),link(电影链接)
  ratings.csv - userId,movieId,rating,timestamp
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import re
import sys
import os
import logging
import csv
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('douban_crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DoubanCSVCrawler:
    def __init__(self):
        """
        初始化爬虫
        """
        # 豆瓣请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        
        # 豆瓣URL
        self.base_url = 'https://movie.douban.com'
        self.top250_url = 'https://movie.douban.com/top250'
        
        # 数据存储
        self.movies_data = []      # 存储movies.csv数据
        self.links_data = []       # 存储links.csv数据
        self.ratings_data = []     # 存储ratings.csv数据
        
        # 电影ID计数器
        self.movie_id_counter = 1
        
        # 统计信息
        self.stats = {
            'total_crawled': 0,
            'movies_saved': 0,
            'links_saved': 0,
            'ratings_saved': 0,
            'failed': 0
        }
    
    def crawl_movie_detail(self, movie_url, movie_id):
        """爬取电影详细信息"""
        try:
            # 随机延迟，避免被封
            time.sleep(random.uniform(1.5, 3.5))
            
            response = requests.get(movie_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # 豆瓣使用utf-8编码
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            movie_info = {
                'movieId': movie_id,
                'title': '',
                'genres': [],
                'img': '',
                'link': movie_url,
                'rating': 0.0,
                'year': None
            }
            
            # 1. 提取电影名称
            title_elem = soup.select_one('h1 span[property="v:itemreviewed"]')
            if title_elem:
                # 分离中文名和年份
                title_text = title_elem.text.strip()
                # 移除年份部分（如果有）
                title_clean = re.sub(r'\s*\(\d{4}\)\s*$', '', title_text)
                movie_info['title'] = title_clean
            
            # 2. 提取年份
            year_elem = soup.select_one('h1 .year')
            if year_elem:
                year_text = year_elem.text.strip()
                year_match = re.search(r'\((\d{4})\)', year_text)
                if year_match:
                    try:
                        movie_info['year'] = int(year_match.group(1))
                    except:
                        movie_info['year'] = None
            else:
                # 如果没有单独的年份元素，尝试从标题中提取
                if title_elem:
                    year_match = re.search(r'\((\d{4})\)', title_elem.text.strip())
                    if year_match:
                        try:
                            movie_info['year'] = int(year_match.group(1))
                        except:
                            movie_info['year'] = None
            
            # 3. 提取评分
            rating_elem = soup.select_one('strong[property="v:average"]')
            if rating_elem:
                try:
                    movie_info['rating'] = float(rating_elem.text.strip())
                except:
                    movie_info['rating'] = 0.0
            
            # 4. 提取图片
            img_elem = soup.select_one('#mainpic img')
            if img_elem:
                movie_info['img'] = img_elem.get('src', '')
            
            # 5. 提取类型（多个用|分隔）
            genre_elems = soup.select('span[property="v:genre"]')
            if genre_elems:
                genres = [g.text.strip() for g in genre_elems]
                movie_info['genres'] = genres
            
            # 6. 提取评分人数（用于生成评分数据）
            rating_count_elem = soup.select_one('span[property="v:votes"]')
            movie_info['rating_count'] = int(rating_count_elem.text.strip()) if rating_count_elem else 0
            
            # 检查必要字段
            if not movie_info['title']:
                logger.warning(f"电影名称缺失，跳过: {movie_url}")
                return None
            
            return movie_info
            
        except Exception as e:
            logger.warning(f"爬取电影详情失败 {movie_url}: {e}")
            return None
    
    def generate_ratings(self, movie_info):
        """为电影生成评分数据"""
        ratings = []
        movie_id = movie_info['movieId']
        rating_value = movie_info['rating']
        rating_count = movie_info.get('rating_count', 0)
        
        if rating_value <= 0 or rating_count <= 0:
            return ratings
        
        # 简单模拟生成一些用户评分数据
        # 实际应用中，这里应该从真实的评分数据中获取
        base_user_id = 1000
        base_timestamp = int(time.time()) - 86400 * 365 * 2  # 两年前
        
        # 根据评分人数生成一定数量的评分
        # 我们限制最多生成50个评分，避免数据量太大
        num_ratings = min(50, max(10, rating_count // 1000))
        
        for i in range(num_ratings):
            user_id = base_user_id + i
            # 在评分基础上添加一些随机变化（±0.5）
            user_rating = max(0.5, min(5.0, rating_value + random.uniform(-0.5, 0.5)))
            # 四舍五入到0.5的倍数（模拟实际评分模式）
            user_rating = round(user_rating * 2) / 2
            
            # 生成时间戳（随机分布在过去两年内）
            rating_timestamp = base_timestamp + random.randint(0, 86400 * 365 * 2)
            
            ratings.append({
                'userId': user_id,
                'movieId': movie_id,
                'rating': user_rating,
                'timestamp': rating_timestamp
            })
        
        return ratings
    
    def crawl_top250_page(self, start=0):
        """爬取豆瓣Top250的一页"""
        try:
            params = {'start': start}
            
            # 随机延迟
            time.sleep(random.uniform(2, 4))
            
            response = requests.get(self.top250_url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            page_movies = []
            items = soup.select('.grid_view .item')
            
            for item in items:
                try:
                    # 获取电影链接
                    link_elem = item.select_one('.hd a')
                    if not link_elem:
                        continue
                    
                    movie_url = link_elem.get('href', '')
                    movie_id = self.movie_id_counter
                    self.movie_id_counter += 1
                    
                    # 爬取电影详情
                    movie_info = self.crawl_movie_detail(movie_url, movie_id)
                    if movie_info:
                        page_movies.append(movie_info)
                        logger.info(f"爬取成功: {movie_info['title']} ({movie_info.get('year', '未知年份')})")
                    
                    # 每个电影之间稍作延迟
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    logger.warning(f"处理电影条目失败: {e}")
                    continue
            
            logger.info(f"第 {start//25 + 1} 页完成，爬取 {len(page_movies)} 部电影")
            return page_movies
            
        except Exception as e:
            logger.error(f"爬取第 {start//25 + 1} 页失败: {e}")
            return []
    
    def crawl_top250(self, max_pages=10):
        """爬取豆瓣Top250电影"""
        logger.info(f"开始爬取豆瓣Top250电影，共 {max_pages} 页")
        
        all_movies = []
        
        for page in range(max_pages):
            start = page * 25
            logger.info(f"正在爬取第 {page + 1} 页 (start={start})...")
            
            movies = self.crawl_top250_page(start)
            all_movies.extend(movies)
            
            self.stats['total_crawled'] += len(movies)
            
            # 每爬完一页后稍作休息
            if page < max_pages - 1:
                sleep_time = random.uniform(5, 10)
                logger.info(f"等待 {sleep_time:.1f} 秒后继续...")
                time.sleep(sleep_time)
        
        logger.info(f"爬取完成，共获得 {len(all_movies)} 部电影信息")
        return all_movies
    
    def process_movie_data(self, movie_info):
        """处理电影数据，生成三个CSV的条目"""
        try:
            movie_id = movie_info['movieId']
            
            # 1. 生成movies.csv数据
            genres_str = '|'.join(movie_info['genres']) if movie_info['genres'] else '(no genres listed)'
            movies_entry = {
                'movieId': movie_id,
                'title': f"{movie_info['title']} ({movie_info.get('year', '')})",
                'genres': genres_str
            }
            self.movies_data.append(movies_entry)
            self.stats['movies_saved'] += 1
            
            # 2. 生成links.csv数据
            links_entry = {
                'movieId': movie_id,
                'img': movie_info['img'],
                'link': movie_info['link']
            }
            self.links_data.append(links_entry)
            self.stats['links_saved'] += 1
            
            # 3. 生成ratings.csv数据
            ratings = self.generate_ratings(movie_info)
            for rating in ratings:
                self.ratings_data.append(rating)
                self.stats['ratings_saved'] += 1
            
            logger.info(f"处理完成: {movie_info['title']} (ID: {movie_id})")
            return True
            
        except Exception as e:
            logger.error(f"处理电影数据失败 {movie_info.get('title', '未知')}: {e}")
            self.stats['failed'] += 1
            return False
    
    def save_to_csv(self):
        """保存数据到CSV文件"""
        try:
            # 1. 保存movies.csv
            if self.movies_data:
                movies_file = 'movies.csv'
                with open(movies_file, 'w', newline='', encoding='utf-8') as f:
                    fieldnames = ['movieId', 'title', 'genres']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.movies_data)
                logger.info(f"已保存 {len(self.movies_data)} 条数据到 {movies_file}")
            
            # 2. 保存links.csv
            if self.links_data:
                links_file = 'links.csv'
                with open(links_file, 'w', newline='', encoding='utf-8') as f:
                    fieldnames = ['movieId', 'img', 'link']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.links_data)
                logger.info(f"已保存 {len(self.links_data)} 条数据到 {links_file}")
            
            # 3. 保存ratings.csv
            if self.ratings_data:
                ratings_file = 'ratings.csv'
                with open(ratings_file, 'w', newline='', encoding='utf-8') as f:
                    fieldnames = ['userId', 'movieId', 'rating', 'timestamp']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.ratings_data)
                logger.info(f"已保存 {len(self.ratings_data)} 条数据到 {ratings_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"保存CSV文件失败: {e}")
            return False
    
    def print_stats(self):
        """打印统计信息"""
        print("\n" + "="*60)
        print("爬虫统计信息")
        print("="*60)
        print(f"总计爬取: {self.stats['total_crawled']} 部电影")
        print(f"保存到 movies.csv: {self.stats['movies_saved']} 条记录")
        print(f"保存到 links.csv: {self.stats['links_saved']} 条记录")
        print(f"保存到 ratings.csv: {self.stats['ratings_saved']} 条记录")
        print(f"失败数量: {self.stats['failed']}")
        
        # 显示一些样例数据
        if self.movies_data:
            print("\n样例数据 (movies.csv):")
            print("-"*40)
            for i, movie in enumerate(self.movies_data[:3], 1):
                print(f"{i}. ID:{movie['movieId']} - {movie['title']}")
                print(f"   类型: {movie['genres']}")
        
        if self.links_data:
            print("\n样例数据 (links.csv):")
            print("-"*40)
            for i, link in enumerate(self.links_data[:2], 1):
                print(f"{i}. ID:{link['movieId']} - 图片: {link['img'][:50]}...")
        
        if self.ratings_data:
            print("\n样例数据 (ratings.csv):")
            print("-"*40)
            for i, rating in enumerate(self.ratings_data[:3], 1):
                timestamp_str = datetime.fromtimestamp(rating['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"{i}. 用户:{rating['userId']} 电影:{rating['movieId']} 评分:{rating['rating']} 时间:{timestamp_str}")
        
        print("="*60)
    
    def run(self, max_pages=10):
        """运行爬虫主程序"""
        logger.info("启动豆瓣电影CSV爬虫...")
        
        try:
            # 1. 爬取数据
            movies = self.crawl_top250(max_pages)
            
            # 2. 处理数据
            logger.info(f"开始处理 {len(movies)} 部电影数据...")
            for i, movie in enumerate(movies, 1):
                self.process_movie_data(movie)
                if i % 10 == 0:
                    logger.info(f"处理进度: {i}/{len(movies)}")
            
            # 3. 保存到CSV
            logger.info("正在保存数据到CSV文件...")
            if not self.save_to_csv():
                logger.error("保存CSV文件失败")
                return False
            
            # 4. 打印统计
            self.print_stats()
            
            return True
            
        except Exception as e:
            logger.error(f"爬虫运行失败: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """主函数"""
    print("豆瓣电影爬虫 - CSV文件生成版本")
    print("="*60)
    print("将生成以下文件：")
    print("  1. movies.csv - 电影基本信息")
    print("  2. links.csv - 电影链接和图片")
    print("  3. ratings.csv - 用户评分数据")
    print("="*60)
    
    # 配置选项
    max_pages = 10  # 爬取页数（每页25部电影）
    
    # 创建爬虫实例
    crawler = DoubanCSVCrawler()
    
    # 运行爬虫
    if crawler.run(max_pages=max_pages):
        print("\n✅ 爬虫运行成功！")
        print("\n生成的文件：")
        print(f"  movies.csv - {len(crawler.movies_data)} 条记录")
        print(f"  links.csv - {len(crawler.links_data)} 条记录")
        print(f"  ratings.csv - {len(crawler.ratings_data)} 条记录")
        
        # 显示文件大小
        for filename in ['movies.csv', 'links.csv', 'ratings.csv']:
            if os.path.exists(filename):
                size = os.path.getsize(filename)
                print(f"  {filename}: {size:,} 字节 ({size/1024:.1f} KB)")
    else:
        print("\n❌ 爬虫运行失败，请查看日志文件 douban_crawler.log")
    
    print("\n程序执行完成！")

if __name__ == "__main__":
    # 检查依赖
    required_packages = ['requests', 'beautifulsoup4']
    
    print("检查 Python 依赖包...")
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} 未安装")
            print(f"  请运行: pip install {package}")
    
    print("\n" + "="*60)
    
    # 运行主程序
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()