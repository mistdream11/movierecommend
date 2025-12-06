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

