记得修改
movierecommend.js
config.py
中的路径

nodejs movierecommend.js
运行服务

提前准备号数据表movierecommend
mysql -u root -p 
启动mysql

```sql
结构如下：
mysql> DESC movieinfo;
+-------------+--------------+------+-----+-------------------+-------------------+
| Field       | Type         | Null | Key | Default           | Extra             |
+-------------+--------------+------+-----+-------------------+-------------------+
| movieid     | int          | NO   | PRI | NULL              | auto_increment    |
| moviename   | varchar(255) | NO   |     | NULL              |                   |
| picture     | varchar(500) | YES  |     | NULL              |                   |
| genre       | varchar(100) | YES  |     | NULL              |                   |
| director    | varchar(100) | YES  |     | NULL              |                   |
| actors      | text         | YES  |     | NULL              |                   |
| year        | int          | YES  |     | NULL              |                   |
| rating      | decimal(3,1) | YES  |     | NULL              |                   |
| description | text         | YES  |     | NULL              |                   |
| created_at  | timestamp    | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+-------------+--------------+------+-----+-------------------+-----------------

mysql> DESC personalratings;
+------------+-------------+------+-----+-------------------+-------------------+
| Field      | Type        | Null | Key | Default           | Extra             |
+------------+-------------+------+-----+-------------------+-------------------+
| id         | int         | NO   | PRI | NULL              | auto_increment    |
| userid     | int         | YES  | MUL | NULL              |                   |
| movieid    | int         | YES  | MUL | NULL              |                   |
| rating     | int         | YES  |     | NULL              |                   |
| timestamp  | varchar(20) | YES  |     | NULL              |                   |
| created_at | timestamp   | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+------------+-------------+------+-----+-------------------+-------------------+

mysql> DESC recommendresult;
+------------+--------------+------+-----+-------------------+-------------------+
| Field      | Type         | Null | Key | Default           | Extra             |
+------------+--------------+------+-----+-------------------+-------------------+
| id         | int          | NO   | PRI | NULL              | auto_increment    |
| userid     | int          | YES  | MUL | NULL              |                   |
| movieid    | int          | YES  | MUL | NULL              |                   |
| rating     | decimal(5,3) | YES  |     | NULL              |                   |
| created_at | timestamp    | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
| moviename  | varchar(255) | YES  |     | NULL              |                   |
+------------+--------------+------+-----+-------------------+-------------------+
6 rows in set (0.00 sec)


mysql> DESC user;
+------------+--------------+------+-----+-------------------+-------------------+
| Field      | Type         | Null | Key | Default           | Extra             |
+------------+--------------+------+-----+-------------------+-------------------+
| userid     | int          | NO   | PRI | NULL              | auto_increment    |
| username   | varchar(50)  | NO   | UNI | NULL              |                   |
| password   | varchar(255) | NO   |     | NULL              |                   |
| email      | varchar(100) | YES  |     | NULL              |                   |
| created_at | timestamp    | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+------------+--------------+------+-----+-------------------+-------------------+
5 rows in set (0.00 sec)
```
# 🎬 电影推荐系统 (Movie Recommendation System)

一个基于**强化学习**和**协同过滤**的电影推荐应用，采用**Node.js + Python**双语言架构，集成了用户认证、电影数据库、个性化评分和智能推荐引擎。

## 📋 项目概述

本项目是一个完整的电影推荐平台，具有以下核心功能：

- ✅ **用户认证系统** - 用户注册、登录、个人中心管理
- ✅ **电影评分模块** - 用户对电影的个性化评分功能
- ✅ **智能推荐引擎** - 基于强化学习和协同过滤的推荐算法

## 🏗️ 项目结构

```
movierecommend/
├── movierecommend.js              # Express.js主程序，处理路由和业务逻辑
├── crawler.py                      # 电影数据爬虫
├── genreLoader.js                  # 电影类型加载器
├── test.py                         # Python测试脚本
│
├── movie_rec/                      # Python推荐引擎模块
│   ├── movie_rec.py               # 强化学习推荐算法核心
│   ├── config.py                  # 配置文件
│   └── db_operations.py           # 数据库操作工具
│
├── views/                          # Web前端视图
│   ├── index.html                 # 首页
│   ├── about.html                 # 搜索
│   ├── loginpage.html             # 登录页面
│   ├── registerpage.html          # 注册页面
|   ├── registersuccess.html       # 注册成功页面
│   ├── modechoice.jade            # 模式选择页面
│   ├── genreselect.jade           # 类型选择页面
│   ├── interactiveRating.jade     # 个人评分页面
|   ├── userscoresuccess           # 评分成功页面
│   ├── recommendresult.jade       # 推荐结果页面
│   └── css/                       # 样式文件
│
├── data/                          # 数据文件
│   ├── movies.csv                 # 电影数据库
│   ├── ratings.csv                # 评分数据
│   └── links.csv                  # 电影链接
│
└── package.json                   # Node.js依赖配置
```

## 🛠️ 技术栈

### 🖥️ 后端技术
| 技术 | 用途 |
|------|------|
| Node.js | JavaScript运行时环境 |
| MySQL 5.7+ | 关系数据库 |
| Python 3.6+ | 推荐算法实现 |
| PySpark | 大规模数据处理 |

### 🎨 前端技术
| 技术 | 用途 |
|------|------|
| HTML5 | 页面结构 |
| CSS3 | 页面样式 |
| Jade | 模板引擎 |
| JavaScript | 客户端交互 |

### 🤖 算法与模型
| 算法 | 说明 |
|------|------|
| **强化学习** | 基于用户反馈的个性化推荐 |
| **协同过滤** | 用户相似度和物品相似度计算 |
| **特征工程** | 特征提取、标准化和选择 |
| **逻辑回归** | 概率预测模型 |

## 📦 安装与配置

### 前提条件
| 软件 | 版本 | 说明 |
|------|------|------|
| Node.js | ≥ 10.0 | JavaScript运行环境 |
| Python | ≥ 3.6 | 推荐算法环境 |
| MySQL | ≥ 5.7 | 数据库服务 |
| npm | ≥ 6.0 | Node包管理器 |
| pip | ≥ 20.0 | Python包管理器 |

### 快速开始

#### 步骤1：克隆项目并安装Node.js依赖

```bash
# 进入项目目录
cd movierecommend

# 安装npm依赖（会自动生成node_modules文件夹）
npm install
```

#### 步骤2：安装Python依赖

```bash
# 使用pip安装必要的Python包
pip install pyspark numpy pandas scipy scikit-learn
```

#### 步骤3：配置数据库

1. **创建MySQL数据库：**

```sql
CREATE DATABASE movierecommend CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE movierecommend;
```

2. **配置数据库连接信息：**

编辑 `movie_rec/config.py` 文件，修改数据库配置：

```python
# 数据库配置
MYSQL_HOST = "localhost"          # MySQL服务器地址
MYSQL_PORT = 3306                 # MySQL服务器端口
MYSQL_DATABASE = "movierecommend" # 数据库名
MYSQL_USER = "root"               # MySQL用户名
MYSQL_PASSWORD = "your_password"  # MySQL密码（修改为你的密码）
```

3. **在Node.js配置中设置MySQL连接：**

在 `movierecommend.js` 中配置MySQL连接参数

#### 步骤4：准备初始数据

将以下数据文件放在项目根目录，或运行爬虫生成：


**或者运行爬虫获取数据：**

```bash
python crawler.py
```

#### 步骤5：初始化数据库表

```bash
python load_into_mysql.py
```

此脚本会自动创建所有必要的数据库表。

## 🚀 使用方法

### 启动服务器

```bash
# 启动Node.js服务器
node movierecommend.js
```

**输出示例：**
```
Server is running on http://localhost:3000
✓ Database connected
✓ Routes loaded
```

访问 `http://localhost:3000` 在浏览器中打开应用。

### 主要功能流程

```
注册/登录 → 选择推荐模式 → 选择电影类型 → 评分电影 → 获取推荐
   ↓           ↓             ↓            ↓         ↓
  用户认证   简单/详细      过滤电影     反馈信息   智能推荐
```

**具体步骤：**

1. **用户注册/登录** - 访问登录页面，使用邮箱或用户名创建账户或登录
2. **选择推荐模式** - 选择"个性化推荐"或"浏览所有电影"
3. **电影类型筛选** - 根据个人偏好选择电影类型（如：动作、喜剧、科幻等）
4. **对电影评分** - 观看推荐的电影并进行1-5星评分
5. **获取推荐结果** - 系统基于评分历史和算法推荐最适合的电影

## 🎨 前端功能详解

### 核心页面与功能

#### 1. 🏠 首页 (index.html)

**功能特性：**
- **轮播图**：展示热门电影推荐，支持自动轮播
- **导航栏**：快速访问登录、注册、关于页面


#### 2. 🎯 推荐模式选择页面 (modechoice.jade)

**功能特性：**
- **双模式选择**：
  - **简单模式** - 5轮选择，快速推荐
  - **详细模式** - 10轮选择，个性化推荐


#### 3. 🎬 电影类型选择页面 (genreselect.jade)

**功能特性：**
- **多类型选择**：支持选择多个电影类型（复选框）


#### 4. ⭐ 个人评分页面 (interactiveRatings.jade)

**功能特性：**
- **电影列表展示**：根据选择的类型显示电影
- **星级评分系统**：喜欢or不喜欢，可剔除未看过的电影


#### 5. 🎯 推荐结果页面 (recommendresult.jade)

**功能特性：**
- **个性化推荐列表**：基于算法生成的推荐电影
- **电影详细信息**



#### 6. 🔍 电影浏览和搜索页面 (about.html)

**功能特性：**
- **电影库展示**：展示系统中所有电影的网格布局
- **实时搜索功能**：输入电影名称实时筛选结果



## 🤖 推荐算法说明

### 采用上下文多臂赌博机(Contextual Bandit)强化学习方法实现个性化电影推荐。系统结合Spark大数据处理框架和MySQL数据库，从用户历史行为中学习并动态优化推荐策略。

### 主要流程
#### 1. 初始化阶段
系统首先解析命令行参数获取数据目录和用户ID，初始化Spark会话用于大数据处理，建立MySQL数据库连接以存取用户评分和推荐结果。

#### 2. 数据加载阶段
从MySQL数据库删除指定用户的旧推荐结果
从personalratings表读取用户的个人评分历史
加载MovieLens数据集，包括ratings.csv（评分数据）和movies.csv（电影元数据）

#### 3. 特征工程阶段

**提取用户特征:** 计算每个用户的平均评分、评分数量和评分方差
**提取电影特征：** 计算每部电影的平均评分、评分数量、平均时间戳和类型数量
对特征进行标准化处理，分别计算用户特征和电影特征的均值和标准差

#### 4. 强化学习模型训练阶段

**初始化强化学习智能体：** 设置状态维度为20，动作为所有电影ID
使用MovieLens历史评分数据进行离线训练，模拟用户与电影的交互过程
采用ε-贪心策略平衡探索与利用，使用线性模型近似Q值函数
实现经验回放机制，从历史经验中随机采样进行批量训练

#### 5. 推荐生成阶段

**对目标用户进行状态编码：** 结合用户特征和最后交互的电影特征生成20维状态向量
**筛选候选电影：** 排除用户已评分的电影，限制候选集大小以提高效率
**计算候选电影的Q值：** 使用训练好的线性模型预测每个动作的价值
选择Q值最高的Top-K电影作为推荐结果，将Q值映射为1-5星的预测评分

#### 6. 结果存储与反馈阶段

将推荐结果插入MySQL的recommendresult表，包含用户ID、电影ID、预测评分和电影名称
在控制台展示推荐列表供用户参考
使用用户的真实评分进行在线学习，更新强化学习模型参数

### 算法核心
系统采用ε-贪心策略进行探索与利用的平衡，使用线性函数逼近Q值，通过时间差分方法更新模型参数。奖励函数基于用户评分设计，值在0-1之间。系统还实现了经验回放机制以提高学习效率和稳定性。



### 数据库表设计

#### 📊 user 表 - 用户表

```sql
CREATE TABLE user (
    userid INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| userid | int | PK, AI | 用户ID（主键，自增） |
| username | varchar(50) | UNIQUE | 用户名（唯一） |
| password | varchar(255) | NOT NULL | 密码（加密存储） |
| email | varchar(100) | NULL | 邮箱地址 |
| created_at | timestamp | DEFAULT | 创建时间 |

#### 📊 movieinfo 表 - 电影表

```sql
CREATE TABLE movieinfo (
    movieid INT PRIMARY KEY AUTO_INCREMENT,
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
);
```

| 字段 | 类型 | 说明 |
|------|------|------|
| movieid | int | 电影ID（主键，自增） |
| moviename | varchar(255) | 电影名称 |
| picture | varchar(500) | 电影海报URL |
| movieurl | varchar(500) | 电影详情页URL |
| genre | varchar(100) | 电影类型 |
| director | varchar(100) | 导演 |
| actors | text | 主演列表 |
| year | int | 上映年份 |
| rating | decimal(3,1) | IMDB评分 |
| description | text | 电影描述 |
| created_at | timestamp | 创建时间 |

#### 📊 personalratings 表 - 个人评分表

```sql
CREATE TABLE personalratings (
    id INT PRIMARY KEY AUTO_INCREMENT,
    userid INT,
    movieid INT,
    rating INT,
    timestamp VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (userid) REFERENCES user(userid),
    FOREIGN KEY (movieid) REFERENCES movieinfo(movieid)
);
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 记录ID（主键） |
| userid | int | 用户ID（外键） |
| movieid | int | 电影ID（外键） |
| rating | int | 用户评分（1-5） |
| timestamp | varchar(20) | 评分时间 |
| created_at | timestamp | 记录创建时间 |

#### 📊 recommendresult 表 - 推荐结果表

```sql
CREATE TABLE recommendresult (
    id INT PRIMARY KEY AUTO_INCREMENT,
    userid INT,
    movieid INT,
    rating DECIMAL(5,3),
    moviename VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (userid) REFERENCES user(userid),
    FOREIGN KEY (movieid) REFERENCES movieinfo(movieid)
);
```

| 字段 | 类型 | 说明 |
|------|------|------|
| id | int | 推荐记录ID（主键） |
| userid | int | 用户ID（外键） |
| movieid | int | 电影ID（外键） |
| rating | decimal(5,3) | 推荐评分（预测值） |
| moviename | varchar(255) | 电影名称 |
| created_at | timestamp | 推荐时间 |



### 常见问题排查

| 问题 | 解决方案 |
|------|---------|
| 数据库连接失败 | 检查MySQL服务是否运行，验证config.py中的连接参数 |
| 找不到node_modules | 运行 `npm install` 安装依赖 |
| 推荐结果为空 | 确保personalratings表中有足够的评分数据 |



## 👥 团队分工

| 团队成员 | 工作职责 |
|---------|---------|
| 王嘉浩 | 前端页面开发、次要功能实现、项目报告 |
| 刘新康 | 数据爬取、核心推荐流程、演示PPT制作 |
| 郑少鸿 | 后端算法模型、视频制作、算法部分报告 |


