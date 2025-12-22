#!/usr/bin/env python3
"""Populate the movierecommend MySQL schema with MovieLens/TMDB exports.

Supported targets (created automatically when missing):
- movieinfo
- `user`
- personalratings
- recommendresult

The script expects MovieLens `movies.csv`, `ratings.csv`, and the enriched
`posters_tmdb.csv` that already contains directors/cast/etc. The recommendresult
rows can either be supplied via a CSV or derived automatically from the highest
ratings per user.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import mysql.connector
from mysql.connector import MySQLConnection

MOVIEINFO_COLUMNS = (
    "movieid",
    "moviename",
    "picture",
    "genre",
    "director",
    "actors",
    "year",
    "rating",
    "description",
    "movieurl",
    "created_at",
)


def get_table_columns(conn: MySQLConnection, table: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    cols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    # 保留原大小写，但查找时按小写匹配
    return cols


def choose_column(table_cols: List[str], candidates: Sequence[str]) -> Optional[str]:
    lower = {c.lower(): c for c in table_cols}
    for name in candidates:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def chunked(rows: Iterable[Sequence], batch_size: int) -> Iterator[List[Sequence]]:
    batch: List[Sequence] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def ensure_tables(conn: MySQLConnection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS movieinfo (
            movieid INT PRIMARY KEY AUTO_INCREMENT,
            moviename VARCHAR(255) NOT NULL,
            picture VARCHAR(500),
            genre VARCHAR(100),
            director VARCHAR(100),
            actors TEXT,
            year INT,
            rating DECIMAL(3,1),
            description TEXT,
            movieurl VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
    )
    # 若表已存在但缺少 movieurl，则补充该列
    try:
        cols = get_table_columns(conn, "movieinfo")
        if "movieurl" not in [c.lower() for c in cols]:
            cursor.execute("ALTER TABLE movieinfo ADD COLUMN movieurl VARCHAR(255)")
    except Exception:
        # 安静忽略，避免不同权限/版本下的异常阻断流程
        pass
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS `user` (
            userid INT PRIMARY KEY AUTO_INCREMENT,
            username VARCHAR(50) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL,
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS personalratings (
            id INT PRIMARY KEY AUTO_INCREMENT,
            userid INT NOT NULL,
            movieid INT NOT NULL,
            rating INT NOT NULL,
            timestamp VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS recommendresult (
            id INT PRIMARY KEY AUTO_INCREMENT,
            userid INT NOT NULL,
            movieid INT NOT NULL,
            rating DECIMAL(5,3) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            moviename VARCHAR(255) NOT NULL
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
    )
    cursor.close()
    conn.commit()


@contextmanager
def open_text_with_fallback(path: Path, encodings: Sequence[str]):
    last_exc: Optional[Exception] = None
    # 遍历所有候选编码
    for enc in encodings:
        if not enc:
            continue
        fh = None
        try:
            # 尝试用当前编码打开文件，添加errors参数兜底（临时容错）
            fh = path.open("r", encoding=enc, newline="", errors="replace")
            yield fh  # 传递文件句柄给with块
            return  # 若读取成功，直接退出函数
        except UnicodeDecodeError as exc:
            last_exc = exc
            continue  # 解码失败，尝试下一个编码
        finally:
            if fh:
                fh.close()  # 无论成功与否，确保文件关闭
    # 所有编码都尝试失败，抛出最后一个错误
    if last_exc is not None:
        raise last_exc from None
    raise RuntimeError(f"No valid encoding found for file: {path}")


def read_movies_csv(movies_csv: Path, encodings: Sequence[str]) -> Dict[int, Dict[str, str]]:
    movie_rows: Dict[int, Dict[str, str]] = {}
    with open_text_with_fallback(movies_csv, encodings) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            movie_id_raw = row.get("movieId") or row.get("movie_id")
            if not movie_id_raw:
                continue
            movie_id = int(movie_id_raw)
            title = row.get("title", "")
            # 解析年份，形如 "Toy Story (1995)" -> 1995
            year_val = None
            if title:
                try:
                    left = title.rfind("(")
                    right = title.rfind(")")
                    if left != -1 and right != -1 and right > left:
                        year_str = title[left + 1:right]
                        year_val = int(year_str)
                except Exception:
                    year_val = None
            movie_rows[movie_id] = {
                "moviename": title,
                "genre": row.get("genres", ""),
                "year": year_val if year_val is not None else "",
            }
    return movie_rows


def merge_posters(
    movie_rows: Dict[int, Dict[str, str]],
    posters_csv: Path,
    encodings: Sequence[str],
) -> None:
    if not posters_csv.exists():
        return
    with open_text_with_fallback(posters_csv, encodings) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            movie_id_raw = row.get("movieId")
            if not movie_id_raw:
                continue
            movie_id = int(movie_id_raw)
            payload = movie_rows.setdefault(movie_id, {"moviename": row.get("title", "")})
            payload.update(
                {
                    "moviename": row.get("title", payload.get("moviename", "")),
                    "picture": row.get("poster_url", ""),
                    "director": row.get("directors", ""),
                    "actors": row.get("main_cast", ""),
                    "rating": row.get("vote_average", ""),
                    "description": row.get("overview", ""),
                    "release_date": row.get("release_date", ""),
                    "rating_count": row.get("vote_count", ""),
                    "movieurl": row.get("tmdb_url", ""),
                }
            )


def insert_movieinfo(conn: MySQLConnection, movies: Dict[int, Dict[str, str]], batch_size: int) -> int:
    if not movies:
        return 0
    table_cols = get_table_columns(conn, "movieinfo")
    # 主键列可能是 movieid 或 movieId
    pk_col = choose_column(table_cols, ["movieid", "movieId"]) or "movieid"

    # 构建可用列的映射
    col_map = []  # (db_col, value_key, converter)
    def add(candidate_names: Sequence[str], value_key: str, conv=None):
        col = choose_column(table_cols, candidate_names)
        if col:
            col_map.append((col, value_key, conv))

    add(["moviename", "movie_name"], "moviename")
    add(["picture", "poster_url"], "picture")
    add(["genre", "genres"], "genre")
    add(["director", "directors"], "director")
    add(["actors", "main_cast"], "actors")
    # TMDB 详情页 URL
    add(["movieurl"], "movieurl")
    # 年份或上映日期
    if choose_column(table_cols, ["year"]):
        add(["year"], "year", lambda v: int(v) if str(v).isdigit() else None)
    elif choose_column(table_cols, ["release_date"]):
        add(["release_date"], "release_date")
    # 评分
    if choose_column(table_cols, ["rating"]):
        add(["rating"], "rating", lambda v: float(v) if v not in (None, "") else None)
    elif choose_column(table_cols, ["avg_rating"]):
        add(["avg_rating"], "rating", lambda v: float(v) if v not in (None, "") else None)
    # 票数（如存在）
    if choose_column(table_cols, ["rating_count"]):
        add(["rating_count"], "rating_count", lambda v: int(float(v)) if str(v or "").strip() else None)
    # 简介
    add(["description", "overview"], "description")

    insert_cols = [pk_col] + [db_col for db_col, _, _ in col_map]
    placeholders = ["%s"] * len(insert_cols)
    update_parts = [f"{db_col}=VALUES({db_col})" for db_col, _, _ in col_map]
    insert_sql = f"INSERT INTO movieinfo ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)}) "
    if update_parts:
        insert_sql += "ON DUPLICATE KEY UPDATE " + ", ".join(update_parts)

    def build_row(mid: int, data: Dict[str, str]):
        vals = [mid]
        for db_col, key, conv in col_map:
            val = data.get(key)
            if conv:
                try:
                    val = conv(val)
                except Exception:
                    val = None
            vals.append(val)
        return tuple(vals)

    rows_iter = (build_row(movie_id, data) for movie_id, data in movies.items())
    cursor = conn.cursor()
    inserted = 0
    for batch in chunked(rows_iter, batch_size):
        cursor.executemany(insert_sql, batch)
        inserted += cursor.rowcount
        conn.commit()
    cursor.close()
    return inserted


def insert_personalratings(
    conn: MySQLConnection,
    ratings_csv: Path,
    batch_size: int,
    encodings: Sequence[str],
) -> Tuple[int, List[int], Dict[int, List[Tuple[int, float]]]]:
    if not ratings_csv.exists():
        return 0, [], {}
    table_cols = get_table_columns(conn, "personalratings")
    user_col = choose_column(table_cols, ["userid", "userId"]) or "userid"
    movie_col = choose_column(table_cols, ["movieid", "movieId"]) or "movieid"
    rating_col = choose_column(table_cols, ["rating"]) or "rating"
    ts_col = choose_column(table_cols, ["timestamp", "rating_timestamp"])  # 可为空
    insert_cols = [user_col, movie_col, rating_col] + ([ts_col] if ts_col else [])
    placeholders = ["%s"] * len(insert_cols)
    insert_sql = f"INSERT INTO personalratings ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
    user_ids: List[int] = []
    recommendations: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
    cursor = conn.cursor()
    inserted = 0
    with open_text_with_fallback(ratings_csv, encodings) as fh:
        reader = csv.DictReader(fh)
        rows_buffer: List[Sequence] = []
        for row in reader:
            if not row.get("userId") or not row.get("movieId") or not row.get("rating"):
                continue
            user_id = int(row["userId"])
            movie_id = int(row["movieId"])
            rating_val = float(row["rating"])  # 兼容 decimal/int
            timestamp_val = row.get("timestamp")
            values = [user_id, movie_id, rating_val]
            if ts_col:
                values.append(str(timestamp_val) if timestamp_val is not None else None)
            rows_buffer.append(tuple(values))
            recommendations[user_id].append((movie_id, rating_val))
            if user_id not in user_ids:
                user_ids.append(user_id)
            if len(rows_buffer) >= batch_size:
                cursor.executemany(insert_sql, rows_buffer)
                inserted += cursor.rowcount
                conn.commit()
                rows_buffer.clear()
        if rows_buffer:
            cursor.executemany(insert_sql, rows_buffer)
            inserted += cursor.rowcount
            conn.commit()
    cursor.close()
    return inserted, user_ids, recommendations


def insert_users(conn: MySQLConnection, user_ids: Iterable[int], default_password: str, batch_size: int) -> int:
    if not user_ids:
        return 0
    table_cols = get_table_columns(conn, "user")
    id_col = choose_column(table_cols, ["userid", "userId"]) or "userid"
    username_col = choose_column(table_cols, ["username"]) or "username"
    password_col = choose_column(table_cols, ["password", "password_hash"]) or "password"
    email_col = choose_column(table_cols, ["email"])  # 可能不存在
    cols = [id_col, username_col, password_col] + ([email_col] if email_col else [])
    placeholders = ["%s"] * len(cols)
    insert_sql = f"INSERT INTO `user` ({', '.join(cols)}) VALUES ({', '.join(placeholders)}) ON DUPLICATE KEY UPDATE {username_col}=VALUES({username_col})"
    password_hash = hashlib.sha256(default_password.encode()).hexdigest()
    def make_row(uid: int):
        base = [uid, f"user_{uid}", password_hash]
        if email_col:
            base.append(None)
        return tuple(base)
    rows_iter = (make_row(user_id) for user_id in user_ids)
    cursor = conn.cursor()
    inserted = 0
    for batch in chunked(rows_iter, batch_size):
        cursor.executemany(insert_sql, batch)
        inserted += cursor.rowcount
        conn.commit()
    cursor.close()
    return inserted


def read_recommendations_csv(
    rec_csv: Path,
    encodings: Sequence[str],
) -> Iterator[Tuple[int, int, float, str]]:
    with open_text_with_fallback(rec_csv, encodings) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if not row.get("userId") or not row.get("movieId"):
                continue
            movie_name = row.get("moviename") or row.get("movieName") or row.get("movie_name") or ""
            yield (
                int(row["userId"]),
                int(row["movieId"]),
                float(row.get("rating", "0")),
                movie_name,
            )


def derive_recommendations_from_ratings(
    recommendations: Dict[int, List[Tuple[int, float]]],
    movies: Dict[int, Dict[str, str]],
    top_k: int,
) -> Iterator[Tuple[int, int, float, str]]:
    for user_id, movie_list in recommendations.items():
        sorted_movies = sorted(movie_list, key=lambda item: item[1], reverse=True)
        for movie_id, score in sorted_movies[:top_k]:
            movie_name = movies.get(movie_id, {}).get("moviename", "")
            yield (user_id, movie_id, score, movie_name)


def insert_recommendations(
    conn: MySQLConnection,
    rec_rows: Iterable[Tuple[int, int, float, str]],
    batch_size: int,
) -> int:
    table_cols = get_table_columns(conn, "recommendresult")
    user_col = choose_column(table_cols, ["userid", "userId"]) or "userid"
    movie_col = choose_column(table_cols, ["movieid", "movieId"]) or "movieid"
    rating_col = choose_column(table_cols, ["rating"]) or "rating"
    name_col = choose_column(table_cols, ["moviename", "movie_name"]) or "moviename"
    insert_cols = [user_col, movie_col, rating_col, name_col]
    placeholders = ["%s"] * len(insert_cols)
    insert_sql = f"INSERT INTO recommendresult ({', '.join(insert_cols)}) VALUES ({', '.join(placeholders)})"
    cursor = conn.cursor()
    inserted = 0
    for batch in chunked(rec_rows, batch_size):
        cursor.executemany(insert_sql, batch)
        inserted += cursor.rowcount
        conn.commit()
    cursor.close()
    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import MovieLens/TMDB exports into MySQL")
    parser.add_argument("--movies", type=Path, default=Path("movies.csv"), help="MovieLens movies.csv path")
    parser.add_argument("--posters", type=Path, default=Path("posters_tmdb.csv"), help="Enhanced metadata CSV")
    parser.add_argument("--ratings", type=Path, default=Path("ratings.csv"), help="MovieLens ratings.csv path")
    parser.add_argument("--recommendations", type=Path, default=None, help="Optional recommendresult CSV")
    parser.add_argument("--auto-top-k", type=int, default=5, help="Top-K ratings to seed recommendresult when CSV missing")
    parser.add_argument("--default-password", default="Password123!", help="Default password for generated users")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", type=int, default=3306)
    parser.add_argument("--db-name", default="movierecommend")
    parser.add_argument("--db-user", default="root")
    parser.add_argument("--db-pass", default="123456")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--csv-encoding", default="utf-8", help="Primary CSV encoding")
    parser.add_argument(
        "--csv-fallback-encoding",
        default="gbk",
        help="Fallback CSV encoding when primary fails",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    encodings = []
    for enc in (args.csv_encoding, args.csv_fallback_encoding):
        if enc and enc.lower() not in [e.lower() for e in encodings]:
            encodings.append(enc)
    if not encodings:
        encodings.append("utf-8")
    conn = mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_pass,
        charset="utf8mb4",
    )
    try:
        ensure_tables(conn)
        movie_rows = read_movies_csv(args.movies, encodings)
        merge_posters(movie_rows, args.posters, encodings)
        movie_count = insert_movieinfo(conn, movie_rows, args.batch_size)
        rating_count, user_ids, rating_lookup = insert_personalratings(
            conn,
            args.ratings,
            args.batch_size,
            encodings,
        )
        user_count = insert_users(conn, user_ids, args.default_password, args.batch_size)
        if args.recommendations and args.recommendations.exists():
            rec_rows = list(
                read_recommendations_csv(
                    args.recommendations,
                    encodings,
                )
            )
        else:
            rec_rows = list(
                derive_recommendations_from_ratings(
                    rating_lookup,
                    movie_rows,
                    args.auto_top_k,
                )
            )
        rec_count = insert_recommendations(conn, rec_rows, args.batch_size)
        print(f"movieinfo rows processed: {movie_count}")
        print(f"personalratings rows processed: {rating_count}")
        print(f"users upserted: {user_count}")
        print(f"recommendresult rows processed: {rec_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
