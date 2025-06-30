import os
import os.path
import re
import subprocess
import tempfile
import time
import datetime
import base64
import logging
import argparse  # 用于处理命令行参数
import math  # 用于对数计算
import sqlite3  # <--- 新增
import json  # <--- 新增，用于处理JSON字符串和对象

import pandas as pd
import requests
from rich import box
from rich.console import Console
from rich.table import Table
from collections import defaultdict

# --- Global Configuration (全局配置，部分可被命令行参数覆盖) ---
OUTPUT_BASE_DIR = "results_default"
DB_FILE = "icp_asset_cache.db"  # <--- 新增：数据库文件名
CACHE_EXPIRY_HOURS = 7 * 24  # <--- 新增：缓存有效期（例如7天）

# --- Rich Console (用于美化终端输出) ---
cs_console = Console(log_path=False)

# --- Global Constants (全局常量) ---
API_KEY = ""  # !!! 请替换为您的有效 Quake API Key !!!
BASE_URL = "https://quake.360.net/api/v3"
INPUT_FILE = "icpCheck.txt"
BATCH_SIZE = 1000  # Quake API每批次获取数量
DELAY = 3
DEFAULT_PORTS = {  # ... (保持不变)
    21, 22, 23, 80, 81, 82, 88, 389, 443, 444, 445, 631, 873, 943, 1099, 1433, 1521, 1883, 1936, 2022, 2049, 2082, 2083,
    2086, 2087, 2095, 2096, 2222, 2375, 2376, 2379, 2483, 2484, 3000, 3306, 3307, 3389, 4000, 4001, 4040, 4502, 4503,
    4848, 5000, 5001, 5003, 5005, 5050, 5432, 5433, 5601, 5683, 5800, 5900, 5901, 5984, 5985, 5986, 6080, 6379, 6443,
    7000, 7001, 7002, 7070, 7071, 7080, 7473, 7474, 7777, 7848, 8000, 8001, 8003, 8005, 8006, 8008, 8009, 8010, 8020,
    8069, 8080, 8081, 8082, 8083, 8086, 8087, 8088, 8089, 8090, 8091, 8096, 8161, 8180, 8200, 8400, 8404, 8443, 8500,
    8501, 8529, 8787, 8800, 8848, 8880, 8883, 8887, 8888, 8980, 8983, 9000, 9001, 9002, 9021, 9042, 9043, 9060, 9080,
    9090, 9091, 9092, 9093, 9200, 9300, 9418, 9443, 9500, 9600, 9848, 9849, 9876, 9990, 9999, 10000, 10001, 10022,
    10080, 10250, 10256, 10257, 10259, 10443, 11211, 13306, 13389, 15432, 15601, 15672, 16379, 18080, 18200, 18500,
    18848, 19000, 19001, 19090, 19200, 20000, 22002, 25672, 26257, 27017, 28017, 30000, 33060, 33890, 35357, 50000,
    50070, 50075, 50090, 53083
}
QUAKE_QUERY_TEMPLATE = 'icp_keywords:"{target}" and icp:"鲁icp" and not domain_is_wildcard:true and country:"China" AND not province:"Hongkong"'
SHOW_SCAN_INFO = False

# # --- 新增Fofa配置 ---
FOFA_EMAIL = ""  # 您的Fofa注册邮箱
FOFA_KEY = ""  # 您的Fofa API Key
FOFA_BASE_URL = "https://fofa.info"  # Fofa API 基础URL

# --- 新增 wer.plus API配置 ---
WERPLUS_API_KEY = ""  # !!! 请替换为您的有效 wer.plus API Key !!!


# ======================= 日志配置函数 =======================
def configure_logging(log_file_path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logging.info(f"日志已配置，将记录到: {log_file_path} (使用UTF-8编码)")


# ======================= 数据库初始化函数 =======================
def initialize_database():
    """(已更新) 初始化数据库，创建所有需要的表结构。"""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # --- Quake和Fofa相关表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Targets (
            target_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_name TEXT UNIQUE NOT NULL,
            last_queried_quake TIMESTAMP,
            last_queried_fofa TIMESTAMP,
            notes TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS QuakeRawData (
            data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id INTEGER NOT NULL,
            query_timestamp TIMESTAMP NOT NULL,
            raw_json TEXT NOT NULL,
            FOREIGN KEY (target_id) REFERENCES Targets (target_id)
        );
        """)
        try:
            cursor.execute("ALTER TABLE Targets ADD COLUMN last_queried_fofa TIMESTAMP;")
            logging.info("成功为 'Targets' 表添加 'last_queried_fofa' 字段。")
        except sqlite3.OperationalError:
            logging.info("'last_queried_fofa' 字段已存在于 'Targets' 表中。")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FofaRuns (
            fofa_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id INTEGER NOT NULL,
            run_timestamp TIMESTAMP NOT NULL,
            status TEXT DEFAULT 'pending',
            input_ip_count INTEGER DEFAULT 0,
            found_results_count INTEGER DEFAULT 0,
            notes TEXT,
            FOREIGN KEY (target_id) REFERENCES Targets (target_id)
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FofaRawData (
            fofa_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fofa_run_id INTEGER NOT NULL,
            raw_json TEXT NOT NULL,
            FOREIGN KEY (fofa_run_id) REFERENCES FofaRuns (fofa_run_id)
        );
        """)

        # --- 新增小程序/APP查询缓存表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS CompanyAppCache (
            cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT UNIQUE NOT NULL,
            last_queried TIMESTAMP NOT NULL,
            raw_json_apps TEXT,
            raw_json_miniprograms TEXT
        );
        """)
        conn.commit()

        # --- 为所有表创建索引 ---
        logging.info("数据库表结构创建/检查完毕，准备创建索引。")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_target_name ON Targets (target_name);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_quakerawdata_target_id_timestamp ON QuakeRawData (target_id, query_timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fofaruns_target_id ON FofaRuns (target_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fofarawdata_run_id ON FofaRawData (fofa_run_id);")
        # --- 为新增的缓存表创建索引 ---
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_company_name_cache ON CompanyAppCache (company_name);")
        conn.commit()

        logging.info(f"数据库 '{DB_FILE}' 初始化成功，所有表和索引已创建或已存在。")
        return conn
    except sqlite3.Error as e:
        logging.error(f"数据库初始化失败: {e}", exc_info=True)
        cs_console.print(f"[bold red]Error:[/bold red] 数据库初始化失败: {e}")
        if conn:
            conn.close()
        return None


# ======================= Shared Helper Functions (通用辅助函数) =======================
def load_queries(file_path):
    """
    (重构后) 从指定文件加载查询目标列表。
    这个函数现在只负责读取文件，不与数据库交互。
    返回一个包含目标字符串的列表。
    """
    logging.info(f"开始从文件加载查询目标，文件路径: {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"输入文件不存在: {file_path}")
        cs_console.print(f"[bold red]Error:[/bold red] 输入文件不存在: {file_path}")
        return []
    if os.path.getsize(file_path) == 0:
        logging.warning(f"输入文件为空: {file_path}")
        cs_console.print(f"[yellow]Warning:[/yellow] 输入文件为空: {file_path}")
        return []

    with open(file_path, 'r', encoding='utf-8') as f:
        # 读取所有行，去除首尾空白，并过滤掉空行
        queries = [line.strip() for line in f if line.strip()]

    if queries:
        logging.info(f"加载了 {len(queries)} 个有效的查询目标。")
        cs_console.print(f"[green]INFO:[/green] 加载了 {len(queries)} 个有效的查询目标从 '{file_path}'。")
    else:
        logging.info("文件中未加载到有效的查询目标。")

    return queries


def create_self_check_report(failed_targets_list, db_conn, mode_name):
    """
    (最终修正版) 创建一个包含两个Sheet的Excel自查报告，并使用正确的换行符。
    """
    if not failed_targets_list and not db_conn:
        return

    report_path = os.path.join(OUTPUT_BASE_DIR, "自查报告.xlsx")
    cs_console.print(f"\n[bold blue]生成自查报告...[/bold blue] -> '{report_path}'")

    try:
        # --- 查询数据库中所有在有效期内的缓存目标 ---
        valid_cached_targets_for_excel = []
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT target_id, target_name, last_queried_quake FROM Targets WHERE last_queried_quake IS NOT NULL")
        all_targets_with_timestamp = cursor.fetchall()

        for target_id, target_name, timestamp_str in all_targets_with_timestamp:
            last_queried_dt = None
            try:
                if '.' in timestamp_str:
                    last_queried_dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                else:
                    last_queried_dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                continue

            cache_age_hours = (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600

            if cache_age_hours < CACHE_EXPIRY_HOURS:
                remaining_hours = round(CACHE_EXPIRY_HOURS - cache_age_hours, 2)

                found_companies = set()
                cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ?", (target_id,))
                raw_json_rows = cursor.fetchall()
                for (raw_json_str,) in raw_json_rows:
                    try:
                        raw_data_obj = json.loads(raw_json_str)
                        unit_name = raw_data_obj.get("service", {}).get("http", {}).get("icp", {}).get("main_licence",
                                                                                                       {}).get("unit",
                                                                                                               "")
                        if unit_name:
                            found_companies.add(unit_name)
                    except json.JSONDecodeError:
                        continue

                # --- 关键修改点：确保使用正确的换行符 "\n" ---
                companies_str = "\n".join(sorted(list(found_companies))) if found_companies else "未发现主体单位"

                valid_cached_targets_for_excel.append({
                    '查询目标': target_name,
                    '包含的备案主体': companies_str,
                    '缓存时间': timestamp_str.split('.')[0],
                    '剩余有效期(小时)': remaining_hours
                })

        # --- 使用pandas将数据写入Excel的不同Sheet ---
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            # Sheet 1: 未正确查询的目标
            if failed_targets_list:
                df_failed = pd.DataFrame(failed_targets_list)
                df_failed.rename(columns={'name': '查询目标', 'reason': '原因'}, inplace=True)
                df_failed.to_excel(writer, sheet_name="未正确查询的目标", index=False)
            else:
                pd.DataFrame([{'状态': '本次运行所有目标的Quake数据均已成功获取'}]).to_excel(writer,
                                                                                             sheet_name="未正确查询的目标",
                                                                                             index=False)
            # Sheet 2: 有效期内的缓存目标
            if valid_cached_targets_for_excel:
                df_valid = pd.DataFrame(valid_cached_targets_for_excel)
                df_valid.to_excel(writer, sheet_name="有效期内的缓存目标", index=False)
            else:
                pd.DataFrame([{'状态': '当前数据库中无有效缓存'}]).to_excel(writer, sheet_name="有效期内的缓存目标",
                                                                            index=False)

        logging.info(f"自查报告已成功生成: {report_path}")
        cs_console.print(f"  [green]Success:[/green] 自查报告已生成。")

    except Exception as e:
        logging.error(f"生成自查报告失败: {e}", exc_info=True)
        cs_console.print(f"  [bold red]Error:[/bold red] 生成自查报告失败 (详情见日志)。")


def check_and_get_quake_cache(target_name, db_conn):
    """
    (新增辅助函数) 检查并尝试从数据库获取指定目标的有效Quake数据缓存。
    - 如果找到有效且未过期的缓存，则解析并返回数据列表。
    - 如果没有缓存或缓存已过期，则返回 None。
    """
    try:
        cursor = db_conn.cursor()
        # 1. 根据目标名称在 Targets 表中查找记录
        cursor.execute("SELECT target_id, last_queried_quake FROM Targets WHERE target_name = ?", (target_name,))
        target_row = cursor.fetchone()

        if not target_row:
            logging.info(f"缓存检查: 目标 '{target_name}' 在数据库中无记录，需要查询API。")
            return None

        target_id, last_queried_quake_str = target_row

        # 2. 检查缓存时间戳是否有效且未过期
        if not last_queried_quake_str:
            logging.info(f"缓存检查: 目标 '{target_name}' (ID: {target_id}) 无有效查询时间，需要查询API。")
            return None

        last_queried_dt = None
        try:
            if '.' in last_queried_quake_str:
                last_queried_dt = datetime.datetime.strptime(last_queried_quake_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_queried_dt = datetime.datetime.strptime(last_queried_quake_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            logging.warning(f"缓存检查: 无法解析目标 '{target_name}' 的时间戳 '{last_queried_quake_str}'，将查询API。")
            return None

        cache_age_hours = (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600
        if cache_age_hours >= CACHE_EXPIRY_HOURS:
            logging.info(f"缓存检查: 目标 '{target_name}' 的缓存已过期 (年龄: {cache_age_hours:.2f} 小时)，需要查询API。")
            return None

        # 3. 如果缓存有效，从 QuakeRawData 表中获取数据
        logging.info(f"缓存检查: 目标 '{target_name}' 的缓存有效，尝试从数据库加载数据...")
        cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ? AND query_timestamp = ?",
                       (target_id, last_queried_dt))
        cached_rows = cursor.fetchall()

        if not cached_rows:
            logging.warning(f"缓存检查: 目标 '{target_name}' (ID: {target_id}) 在QuakeRawData中未找到记录，将查询API。")
            return None

        # 4. 解析并返回缓存数据
        raw_json_list_from_cache = [row[0] for row in cached_rows]
        parsed_data_from_cache = parse_results([json.loads(r) for r in raw_json_list_from_cache])

        cs_console.print(
            f"    [green]缓存命中:[/green] '{target_name}' 从数据库加载并解析 {len(parsed_data_from_cache)} 条Quake记录。")
        logging.info(f"成功从数据库缓存为目标 '{target_name}' 加载并解析了 {len(parsed_data_from_cache)} 条记录。")

        return parsed_data_from_cache

    except sqlite3.Error as e:
        logging.error(f"检查Quake缓存时数据库出错 (目标: {target_name}): {e}", exc_info=True)
        cs_console.print(f"[bold red]Error:[/bold red] 检查缓存时数据库出错 ({target_name}): {e}")
        return None


def check_and_get_fofa_cache(target_id, db_conn):
    """
    (新增) 检查并尝试从数据库获取指定目标的有效Fofa数据缓存。
    - 如果找到有效且未过期的缓存，则解析并返回数据列表。
    - 如果没有缓存或缓存已过期，则返回 None。
    """
    try:
        cursor = db_conn.cursor()
        # 1. 根据目标ID在 Targets 表中查找 Fofa 缓存时间戳
        cursor.execute("SELECT last_queried_fofa FROM Targets WHERE target_id = ?", (target_id,))
        target_row = cursor.fetchone()

        if not target_row or not target_row[0]:
            logging.info(f"Fofa缓存检查: 目标ID '{target_id}' 无Fofa查询记录，需要查询API。")
            return None

        last_queried_fofa_str = target_row[0]

        # 2. 检查缓存时间戳是否有效且未过期
        last_queried_dt = None
        try:
            if '.' in last_queried_fofa_str:
                last_queried_dt = datetime.datetime.strptime(last_queried_fofa_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_queried_dt = datetime.datetime.strptime(last_queried_fofa_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            logging.warning(f"Fofa缓存检查: 无法解析目标ID '{target_id}' 的时间戳 '{last_queried_fofa_str}'，将查询API。")
            return None

        cache_age_hours = (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600
        if cache_age_hours >= CACHE_EXPIRY_HOURS:
            logging.info(
                f"Fofa缓存检查: 目标ID '{target_id}' 的缓存已过期 (年龄: {cache_age_hours:.2f} 小时)，需要查询API。")
            return None

        # 3. 如果缓存有效，从 FofaRawData 表中获取数据
        logging.info(f"Fofa缓存检查: 目标ID '{target_id}' 的缓存有效，尝试从数据库加载数据...")

        # 找到与该目标关联的、时间戳最接近的、成功的Fofa运行批次
        cursor.execute("""
            SELECT fr.fofa_run_id FROM FofaRuns fr
            WHERE fr.target_id = ? AND fr.status = 'completed'
            ORDER BY fr.run_timestamp DESC LIMIT 1
        """, (target_id,))
        fofa_run_row = cursor.fetchone()

        if not fofa_run_row:
            logging.warning(f"Fofa缓存检查: 未找到目标ID '{target_id}' 对应的成功Fofa运行记录，将查询API。")
            return None

        fofa_run_id = fofa_run_row[0]
        cursor.execute("SELECT raw_json FROM FofaRawData WHERE fofa_run_id = ?", (fofa_run_id,))
        cached_rows = cursor.fetchall()

        if not cached_rows:
            logging.warning(f"Fofa缓存检查: 目标ID '{target_id}' 在FofaRawData中未找到记录，将查询API。")
            return None

        # 4. 解析并返回缓存数据 (Fofa数据分块存储，需合并)
        raw_results_from_cache = []
        for (raw_json_str,) in cached_rows:
            chunk = json.loads(raw_json_str)
            raw_results_from_cache.extend(chunk)

        parsed_data_from_cache = parse_fofa_results(raw_results_from_cache)

        cs_console.print(
            f"    [green]Fofa缓存命中:[/green] 从数据库加载并解析 {len(parsed_data_from_cache)} 条Fofa记录。")
        logging.info(f"成功从数据库为目标ID '{target_id}' 加载并解析了 {len(parsed_data_from_cache)} 条Fofa记录。")
        return parsed_data_from_cache

    except sqlite3.Error as e:
        logging.error(f"检查Fofa缓存时数据库出错 (目标ID: {target_id}): {e}", exc_info=True)
        cs_console.print(f"[bold red]Error:[/bold red] 检查Fofa缓存时数据库出错 (ID: {target_id}): {e}")
        return None


def query_all_pages(target_name, db_conn):
    """
    (最终修正版) 仅负责实时查询Quake API，并根据官方文档的终止条件正确结束循环。
    成功获取数据后，将目标和数据存入数据库。
    """
    cs_console.print(f"    [blue]API查询:[/blue] 目标 '{target_name}'，开始通过Quake API获取数据...")
    headers = {"X-QuakeToken": API_KEY, "Content-Type": "application/json"}
    current_query_dsl = QUAKE_QUERY_TEMPLATE.format(target=target_name)
    logging.info(f"构造的Quake查询语句 (目标: {target_name}): {current_query_dsl}")

    base_query_params = {"query": current_query_dsl, "size": BATCH_SIZE, "ignore_cache": False,
                         "latest": True}

    all_raw_data_from_api = []

    # --- 循环逻辑修改 ---
    current_pagination_id = None
    previous_pagination_id = "-1"  # 初始化为一个不可能与API返回ID相同的值
    page_count = 0
    api_call_successful = False

    try:
        while True:
            page_count += 1
            query_params_for_api = base_query_params.copy()
            if current_pagination_id:
                query_params_for_api["pagination_id"] = current_pagination_id

            logging.debug(f"查询Quake API: {target_name}, page: {page_count}, pagination_id: {current_pagination_id}")
            response = requests.post(f"{BASE_URL}/scroll/quake_service", headers=headers, json=query_params_for_api,
                                     timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != 0:
                logging.error(f"Quake API 查询失败 (目标: {target_name}): {result.get('message')}")
                cs_console.print(
                    f"[bold red]Error:[/bold red] Quake API 查询失败 ({target_name}): {result.get('message')}")
                api_call_successful = False
                break

            current_batch_data = result.get("data", [])

            # --- 关键：严格按照API文档的终止条件 ---
            # 获取新的 pagination_id
            next_pagination_id = result.get("meta", {}).get("pagination_id")

            # 检查终止条件: data为空 且 pagination_id 不再变化
            if not current_batch_data and next_pagination_id == current_pagination_id:
                logging.info(f"Quake API 查询完成 (目标: {target_name})，因data为空且pagination_id不变。")
                api_call_successful = True
                break

            # 如果还有数据，则添加到总列表
            if current_batch_data:
                all_raw_data_from_api.extend(current_batch_data)

            # 如果API不再返回 pagination_id，也认为查询结束
            if not next_pagination_id:
                logging.info(f"Quake API 查询完成 (目标: {target_name})，无更多分页ID。")
                api_call_successful = True
                break

            # 更新 pagination_id 用于下一次循环
            previous_pagination_id = current_pagination_id
            current_pagination_id = next_pagination_id

            time.sleep(DELAY)

    except requests.exceptions.RequestException as e:
        logging.error(f"Quake API 请求异常 (目标: {target_name}): {e}")
        cs_console.print(f"[bold red]Error:[/bold red] Quake API 请求异常 ({target_name}): {str(e)}")
        api_call_successful = False
    except Exception as e:
        logging.error(f"Quake API 处理中发生未知异常 (目标: {target_name}): {e}", exc_info=True)
        cs_console.print(f"[bold red]Error:[/bold red] Quake API 未知异常 ({target_name}): {str(e)}")
        api_call_successful = False

    # --- 数据库操作 (逻辑保持不变) ---
    if api_call_successful and all_raw_data_from_api:
        logging.info(
            f"Quake API 查询成功 (目标: {target_name}), 共获取 {len(all_raw_data_from_api)} 条原始记录。准备写入数据库...")
        try:
            cursor = db_conn.cursor()
            current_api_query_timestamp = datetime.datetime.now()

            cursor.execute("SELECT target_id FROM Targets WHERE target_name = ?", (target_name,))
            row = cursor.fetchone()
            if row:
                target_id = row[0]
                cursor.execute("DELETE FROM QuakeRawData WHERE target_id = ?", (target_id,))
                logging.info(f"目标 '{target_name}' (ID: {target_id}) 的旧Quake缓存数据已清理。")
            else:
                cursor.execute("INSERT INTO Targets (target_name) VALUES (?)", (target_name,))
                target_id = cursor.lastrowid
                logging.info(f"新目标 '{target_name}' 已插入数据库，ID: {target_id}")

            data_to_insert_into_db = []
            for single_raw_item in all_raw_data_from_api:
                data_to_insert_into_db.append((
                    target_id,
                    current_api_query_timestamp,
                    json.dumps(single_raw_item, ensure_ascii=False)
                ))

            cursor.executemany("INSERT INTO QuakeRawData (target_id, query_timestamp, raw_json) VALUES (?, ?, ?)",
                               data_to_insert_into_db)

            cursor.execute("UPDATE Targets SET last_queried_quake = ? WHERE target_id = ?",
                           (current_api_query_timestamp, target_id))
            db_conn.commit()
            logging.info(f"目标 '{target_name}' (ID: {target_id}) 的数据库记录和缓存时间戳已成功更新。")

            return parse_results(all_raw_data_from_api)

        except sqlite3.Error as e:
            logging.error(f"将API数据存入数据库时出错 (目标: {target_name}): {e}", exc_info=True)
            cs_console.print(f"[bold red]Error:[/bold red] 存入数据库失败 ({target_name}): {e}")
            db_conn.rollback()
            return parse_results(all_raw_data_from_api)

    elif api_call_successful and not all_raw_data_from_api:
        logging.info(f"Quake API 查询成功，但目标 '{target_name}' 未返回任何数据。")
        cs_console.print(f"    [yellow]无结果:[/yellow] 目标 '{target_name}' 无Quake API查询结果。")
        return []
    else:
        logging.warning(f"由于API查询失败，不为目标 '{target_name}' 执行任何数据库操作。")
        return None


def gather_quake_data(target_names, db_conn):
    """
    (新增核心模块化函数) 封装了Quake数据的完整获取与聚合流程。
    - 遍历所有目标。
    - 检查缓存或调用API获取数据。
    - 聚合所有成功获取的资产。
    - 收集所有失败或无结果的目标。
    - 返回聚合后的资产字典和失败目标列表。
    """
    all_aggregated_assets = defaultdict(lambda: {"ips": set(), "urls": set(), "allPort": set(), "parsed_data": []})
    failed_targets = []

    cs_console.print(f"\n[bold blue]阶段一: Quake数据查询与处理[/bold blue] (共 {len(target_names)} 个目标)")
    for index, target_name in enumerate(target_names, 1):
        cs_console.print(f"  ({index}/{len(target_names)}) 处理查询目标: '{target_name}'...")
        logging.info(f"核心数据获取: 开始处理查询目标: {target_name} ({index}/{len(target_names)})")

        parsed_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_data is None:
            parsed_data = query_all_pages(target_name, db_conn)

        if parsed_data is not None:
            if not parsed_data:
                cs_console.print(f"    [yellow]无数据:[/yellow] 目标 '{target_name}' 查询成功但未返回任何资产。")
                failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'})
            else:
                for item in parsed_data:
                    company_name_key = item.get("主体单位") or "未知主体单位"
                    ip_addr = item.get("IP")
                    port_val = item.get("Port")
                    url = item.get("URL")

                    # 总是聚合最全的数据结构 (包括端口)
                    all_aggregated_assets[company_name_key]["parsed_data"].append(item)
                    if ip_addr: all_aggregated_assets[company_name_key]["ips"].add(ip_addr)
                    if url: all_aggregated_assets[company_name_key]["urls"].add(url)
                    if port_val: all_aggregated_assets[company_name_key]["allPort"].add(str(port_val))

                if parsed_data:
                    cs_console.print(
                        f"    [green]数据处理完成:[/green] '{target_name}' 共处理 {len(parsed_data)} 条资产记录。")
        else:
            cs_console.print(f"    [bold red]查询失败:[/bold red] 目标 '{target_name}' API查询过程出错，已跳过。")
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败'})

    return all_aggregated_assets, failed_targets


def parse_results(raw_data_list_objs):  # 参数名修改以反映其是对象列表
    """解析从Quake API获取的原始数据对象列表，提取关键信息并结构化。"""
    parsed_results = []
    if not raw_data_list_objs:  # 增加空检查
        return parsed_results

    for raw_data_obj in raw_data_list_objs:  # raw_data_obj 是单个Python字典
        service_info = raw_data_obj.get("service", {});
        http_info = service_info.get("http", {})
        icp_info = http_info.get("icp", {});
        main_icp = icp_info.get("main_licence", {}) if icp_info else {}  # 添加icp_info检查
        location_info = raw_data_obj.get("location", {});

        urls = []
        # 尝试从 service.http.http_load_url 获取
        if "http_load_url" in http_info and http_info["http_load_url"]:
            urls.extend(http_info["http_load_url"])
        # 备选：尝试从 service.http.url 获取 (如果存在且不同)
        elif "url" in http_info and http_info["url"] and http_info["url"] not in urls:
            urls.append(http_info["url"])
        # 备选：如果 service.http 不存在，但 service.tls.sni 或 service.hostname 存在，且端口是常见HTTP/S端口
        elif "hostname" in service_info and raw_data_obj.get("port") in [80, 443, 8000, 8080, 8443]:
            protocol = "https" if raw_data_obj.get("port") in [443, 8443] else "http"
            host_for_url = service_info["hostname"]
            if host_for_url:  # 确保 hostname 非空
                urls.append(f"{protocol}://{host_for_url}:{raw_data_obj.get('port')}")

        url_to_use = urls[0] if urls else "";

        raw_title = http_info.get("title", "")
        clean_title = raw_title.replace("\n", " ").replace("\r", " ").strip() if raw_title else ""  # 添加raw_title检查

        ip = raw_data_obj.get("ip", "")
        port = str(raw_data_obj.get("port", ""));  # 确保是字符串
        unit = main_icp.get("unit", "")
        province = location_info.get("province_cn", "")

        parsed = {"IP": ip, "Port": port, "Host": http_info.get("host", ""),
                  "HTTP状态码": http_info.get("status_code", ""), "URL": url_to_use,
                  "Domain": raw_data_obj.get("domain", ""), "网站标题": clean_title,
                  "备案号": icp_info.get("licence", "") if icp_info else "",
                  "主体单位": unit,
                  "备案单位类型": main_icp.get("nature", ""),
                  "时间": raw_data_obj.get("time", ""),  # Quake原始数据中的时间戳
                  "归属省份": province}
        parsed_results.append(parsed)
    return parsed_results


# ======================= IP 智能筛选函数 (OR逻辑 - 最终推荐版 v2) =======================
def identify_shared_service_ips(raw_quake_data_list):
    """
    分析Quake原始数据，识别并返回疑似共享/公共服务的IP集合。
    该版本使用“或”逻辑，只要满足任意一个共享服务特征，其IP就会被排除。
    更新：将product_type的判断从“全等”修改为“包含”。
    """
    shared_service_ips = set()

    # 判断条件一的参数：SAN数量阈值
    SAN_THRESHOLD = 20

    # 判断条件三的参数：公共服务商的CNAME关键词列表
    PUBLIC_SERVICE_KEYWORDS = [
        # 精确的邮件服务商CNAME
        'qiye.aliyun.com', 'exmail.qq.com', 'qiye.163.com',
        'ali-mail.com', 'dingtalk.com', 'mxhichina.com',

        # 精确的CDN服务商CNAME特征
        '.cdn.cloudflare.net', '.akamaiedge.net', '.fastly.net',
        '.chinacache.com', '.cdnetworks.net', 'aliyuncs.com',

        # 常见PaaS/SaaS平台
        'bspapp.com', 'hiflow.tencent.com'
    ]

    for raw_data in raw_quake_data_list:
        ip = raw_data.get("ip")
        if not ip:
            continue

        # --- 判断条件一: 检查证书SAN数量 (高优先级) ---
        try:
            san_list_len = len(
                raw_data['service']['tls']['certificate']['parsed']['extensions']['subject_alt_name']['dns_names'])
            if san_list_len > SAN_THRESHOLD:
                logging.info(f"筛选IP: {ip} 因为其证书SAN数量为 {san_list_len} (> {SAN_THRESHOLD})，判定为共享服务。")
                shared_service_ips.add(ip)
                continue
        except (KeyError, TypeError):
            pass

        # --- 判断条件二：检查Quake识别的产品类型 ---
        try:
            components = raw_data.get('components', [])
            for component in components:
                # 检查 product_type 是否为 CDN (修改点：使用 in 判断)
                prod_type = component.get('product_type', [])
                if "内容分发网络(CDN)" in prod_type:
                    logging.info(f"筛选IP: {ip} 因为其 product_type 包含 '内容分发网络(CDN)'。")
                    shared_service_ips.add(ip)
                    break  # 找到一个匹配，无需再检查这个资产的其他组件

                # 检查 product_name_cn 是否包含 "企业邮箱"
                prod_name = component.get('product_name_cn', '')
                if '企业邮箱' in prod_name:
                    logging.info(f"筛选IP: {ip} 因为其 product_name_cn '{prod_name}' 包含 '企业邮箱'。")
                    shared_service_ips.add(ip)
                    break

            if ip in shared_service_ips:
                continue  # 如果被以上规则加入，则跳过后续检查
        except (KeyError, TypeError):
            pass

        # --- 判断条件三: 检查CNAME记录 ---
        try:
            cname_records = raw_data.get('service', {}).get('dns', {}).get('cname', [])
            if cname_records:
                for record in cname_records:
                    record_lower = record.lower()
                    for keyword in PUBLIC_SERVICE_KEYWORDS:
                        if keyword in record_lower:
                            logging.info(f"筛选IP: {ip} 因为其CNAME '{record}' 指向已知共享服务 '{keyword}'。")
                            shared_service_ips.add(ip)
                            break
                    if ip in shared_service_ips:
                        break
                if ip in shared_service_ips:
                    continue
        except (KeyError, TypeError):
            pass

        # --- 判断条件四: 检查HTTP响应头特征 ---
        try:
            headers = raw_data.get('service', {}).get('http', {}).get('response_headers', '')
            if 'Aliyun URL Forwarding Server' in headers:
                logging.info(f"筛选IP: {ip} 因为其HTTP响应头包含 'Aliyun URL Forwarding Server'，判定为共享转发服务。")
                shared_service_ips.add(ip)
                continue
        except (KeyError, TypeError):
            pass

    return shared_service_ips


def query_fofa_by_ips(ip_list, target_id, db_conn):
    """
    (已更新) 根据IP列表查询Fofa，使用 search/next 接口实现连续翻页，获取所有结果。
    """
    if not ip_list:
        return [], None

    # --- 1. 创建 FofaRuns 运行记录 (逻辑保持不变) ---
    fofa_run_id = None
    current_run_timestamp = datetime.datetime.now()
    cursor = db_conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO FofaRuns (target_id, run_timestamp, status, input_ip_count)
            VALUES (?, ?, ?, ?)
        """, (target_id, current_run_timestamp, 'running', len(ip_list)))
        db_conn.commit()
        fofa_run_id = cursor.lastrowid
        logging.info(f"Fofa: 已创建IP反查运行记录 FofaRuns.fofa_run_id: {fofa_run_id} (目标ID: {target_id})")
    except sqlite3.Error as e:
        logging.error(f"Fofa: 创建FofaRuns记录失败: {e}", exc_info=True)
        return [], None

    # --- 2. 构造查询并分块 (逻辑保持不变) ---
    ip_chunks = [ip_list[i:i + 100] for i in range(0, len(ip_list), 100)]
    all_fofa_raw_results = []
    api_call_overall_successful = True

    cs_console.print(f"    [blue]Fofa查询:[/blue] 准备对 {len(ip_list)} 个IP分 {len(ip_chunks)} 批进行反查...")

    for index, chunk in enumerate(ip_chunks):
        if not api_call_overall_successful: break

        query_str = " || ".join([f'ip="{ip}"' for ip in chunk])
        qbase64 = base64.b64encode(query_str.encode('utf-8')).decode('utf-8')

        # <--- 核心修改点：从基于 page 的翻页，改为基于 next_id 的翻页 ---
        next_id = None  # 初始化 next_id，用于第一次请求
        page_size = 2000  # 使用接口允许的最大查询数量

        cs_console.print(f"      > Fofa查询批次 ({index + 1}/{len(ip_chunks)})...")

        while True:
            try:
                fields = "host,ip,port,protocol,title,server,icp,domain,link"
                # 使用 search/next 接口
                api_url = f"{FOFA_BASE_URL}/api/v1/search/next?email={FOFA_EMAIL}&key={FOFA_KEY}&qbase64={qbase64}&fields={fields}&size={page_size}"

                # 如果不是第一页，则附加 next 参数
                if next_id:
                    api_url += f"&next={next_id}"

                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                result = response.json()

                if result.get("error"):
                    logging.error(f"Fofa API 返回错误 (查询: {query_str}): {result.get('errmsg')}")
                    cs_console.print(f"    [bold red]Error:[/bold red] Fofa API 返回错误: {result.get('errmsg')}")
                    api_call_overall_successful = False
                    break

                batch_results = result.get("results", [])
                if batch_results:
                    all_fofa_raw_results.extend(batch_results)

                # 获取下一次翻页的 ID
                next_id = result.get("next")

                # 终止条件：如果 API 不再返回 next 值，说明已到最后一页
                if not next_id:
                    break

                time.sleep(DELAY)

            except requests.exceptions.RequestException as e:
                logging.error(f"Fofa API 请求异常 (查询: {query_str}): {e}", exc_info=True)
                cs_console.print(f"    [bold red]Error:[/bold red] Fofa API 请求异常: {e}")
                api_call_overall_successful = False
                break
        # <--- 修改结束 --->

    # --- 3. 根据结果更新数据库 (逻辑保持不变) ---
    final_status = 'failed'
    run_notes = ""
    if api_call_overall_successful:
        if all_fofa_raw_results:
            try:
                # 将结果分块存储，避免单个JSON过大
                chunk_size = 100
                data_chunks = [all_fofa_raw_results[i:i + chunk_size] for i in
                               range(0, len(all_fofa_raw_results), chunk_size)]
                data_to_insert = [(fofa_run_id, json.dumps(chunk, ensure_ascii=False)) for chunk in data_chunks]

                cursor.executemany("INSERT INTO FofaRawData (fofa_run_id, raw_json) VALUES (?, ?)", data_to_insert)
                final_status = 'completed'
            except sqlite3.Error as e:
                final_status = 'completed_with_errors'
                run_notes = f"DB insert error: {str(e)[:200]}"
        else:
            final_status = 'completed'
            run_notes = "Query successful but returned no results."
    else:
        run_notes = "API call failed during execution."

    try:
        cursor.execute("UPDATE FofaRuns SET status = ?, found_results_count = ?, notes = ? WHERE fofa_run_id = ?",
                       (final_status, len(all_fofa_raw_results), run_notes, fofa_run_id))

        if final_status.startswith('completed'):
            current_fofa_query_timestamp = datetime.datetime.now()
            cursor.execute("UPDATE Targets SET last_queried_fofa = ? WHERE target_id = ?",
                           (current_fofa_query_timestamp, target_id))
            logging.info(f"Fofa: 成功更新目标ID {target_id} 的Fofa缓存时间戳。")

        db_conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Fofa: 更新FofaRuns状态或Targets时间戳失败: {e}", exc_info=True)
        db_conn.rollback()

    if len(all_fofa_raw_results) > 0:
        cs_console.print(f"    [green]Success:[/green] Fofa查询完成，共获取 {len(all_fofa_raw_results)} 条记录。")

    return all_fofa_raw_results, fofa_run_id


def parse_fofa_results(fofa_raw_data_list):
    """
    (已修复URL拼接Bug) 解析Fofa API返回的原始数据列表。
    - 优先使用 'link' 字段作为URL。
    - 在拼接URL时，正确处理host字段已包含端口的情况。
    """
    parsed_results = []
    fields_order = ["host", "ip", "port", "protocol", "title", "server", "icp", "domain", "link"]

    for item_list in fofa_raw_data_list:
        fofa_item = dict(zip(fields_order, item_list))

        # 优先使用 'link' 字段作为URL
        url = fofa_item.get("link", "")

        # 如果link字段为空，才尝试用旧逻辑拼接URL作为备用方案
        if not url:
            protocol = fofa_item.get("protocol", "").lower()
            host = fofa_item.get("host", "")
            port = fofa_item.get("port", 80)

            # 如果host字段本身就是完整的URL，直接使用
            if "://" in host:
                url = host
            elif protocol and host:
                # <--- 核心修改点：在这里增加判断 --->
                # 如果host字段本身已经包含了端口，则不再拼接
                if ":" in host:
                    url = f"{protocol}://{host}"
                else:
                    # 否则，按原逻辑判断是否需要拼接端口
                    if (protocol == 'http' and port == 80) or (protocol == 'https' and port == 443):
                        url = f"{protocol}://{host}"
                    else:
                        url = f"{protocol}://{host}:{port}"

        # 将Fofa数据转换为脚本内部的标准字典格式
        parsed = {
            "IP": fofa_item.get("ip", ""),
            "Port": str(fofa_item.get("port", "")),
            "Host": fofa_item.get("host", ""),
            "HTTP状态码": "",
            "URL": url,
            "Domain": fofa_item.get("domain", ""),
            "网站标题": fofa_item.get("title", ""),
            "备案号": fofa_item.get("icp", ""),
            "主体单位": "",
            "备案单位类型": "",
            "时间": "",
            "归属省份": "",
            "Server": fofa_item.get("server", "")
        }
        parsed_results.append(parsed)

    return parsed_results


# ======================= 小程序/APP查询相关函数 (最终版 - 支持新API、缓存、参数化) =======================
def _fetch_icpb_data(company_name, app_type):
    """(内部辅助函数) 调用api2.wer.plus接口，分页获取指定类型的数据。"""
    api_url = "https://api2.wer.plus/api/icpb"
    all_results = []
    page = 1
    pagesize = 40

    while True:
        params = {
            'key': WERPLUS_API_KEY,
            't': company_name,
            'page': page,
            'pagesize': pagesize,
            'apptype': app_type
        }
        try:
            response = requests.get(api_url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            if data.get("code") == 200 and data.get("data"):
                current_page_results = data["data"].get("list", [])
                if not current_page_results:
                    break
                all_results.extend(current_page_results)
                if len(all_results) >= data['data'].get('total', 0):
                    break
                page += 1
            else:
                logging.warning(f"查询 {app_type} 失败 (公司: {company_name}): {data.get('msg')}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"请求 {app_type} 接口时异常 (公司: {company_name}): {e}")
            break

    return all_results


def check_and_get_app_cache(company_name, db_conn):
    """检查并获取指定公司的APP/小程序缓存。"""
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT last_queried, raw_json_apps, raw_json_miniprograms FROM CompanyAppCache WHERE company_name = ?",
            (company_name,))
        row = cursor.fetchone()
        if not row: return None

        last_queried_str, raw_apps, raw_miniprograms = row
        last_queried_dt = datetime.datetime.strptime(last_queried_str, '%Y-%m-%d %H:%M:%S.%f')

        cache_age_hours = (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600
        if cache_age_hours >= CACHE_EXPIRY_HOURS:
            logging.info(f"APP缓存检查: '{company_name}' 的缓存已过期。")
            return None

        cs_console.print(f"    [green]APP缓存命中:[/green] 从数据库加载 '{company_name}' 的小程序/APP数据。")
        apps = json.loads(raw_apps) if raw_apps else []
        miniprograms = json.loads(raw_miniprograms) if raw_miniprograms else []
        return {"apps": apps, "miniprograms": miniprograms}
    except (sqlite3.Error, ValueError, TypeError) as e:
        logging.error(f"检查APP缓存时出错 (公司: {company_name}): {e}")
        return None


def query_apps_and_miniprograms(company_name, db_conn, types_to_check):
    """根据传入的types_to_check列表，查询APP/MAPP，并集成智能缓存。"""
    cs_console.print(f"    [blue]额外信息查询:[/blue] 准备查询 '{company_name}' 的 {types_to_check}...")

    app_results, miniprogram_results = [], []
    cached_data = check_and_get_app_cache(company_name, db_conn)

    if cached_data:
        if 'app' in types_to_check: app_results = cached_data.get("apps", [])
        if 'mapp' in types_to_check: miniprogram_results = cached_data.get("miniprograms", [])
    else:
        cs_console.print(f"      > 缓存未命中，执行实时API查询...")
        live_apps, live_miniprograms = [], []

        # 即使只请求一种，也把两种都查出来并存入缓存，以提高后续效率
        cs_console.print(f"      > 正在查询 APP (apptype=app)...")
        live_apps = _fetch_icpb_data(company_name, 'app')
        cs_console.print(f"      > 正在查询 小程序 (apptype=mapp)...")
        live_miniprograms = _fetch_icpb_data(company_name, 'mapp')

        try:
            cursor = db_conn.cursor()
            query_time = datetime.datetime.now()
            cursor.execute("""
                INSERT OR REPLACE INTO CompanyAppCache (company_name, last_queried, raw_json_apps, raw_json_miniprograms)
                VALUES (?, ?, ?, ?)
            """, (company_name, query_time, json.dumps(live_apps), json.dumps(live_miniprograms)))
            db_conn.commit()
            logging.info(f"已将 '{company_name}' 的APP/小程序查询结果写入缓存。")
        except sqlite3.Error as e:
            logging.error(f"写入APP缓存失败 (公司: {company_name}): {e}")
            db_conn.rollback()

        if 'app' in types_to_check: app_results = live_apps
        if 'mapp' in types_to_check: miniprogram_results = live_miniprograms

    combined_results = []
    if app_results:
        for item in app_results:
            item['detected_type'] = 'app'
            combined_results.append(item)
    if miniprogram_results:
        for item in miniprogram_results:
            item['detected_type'] = 'mapp'
            combined_results.append(item)

    if combined_results:
        cs_console.print(f"    [green]查询成功:[/green] 本次获取到 {len(combined_results)} 条记录。")
    elif not cached_data:
        cs_console.print(f"    [yellow]无结果:[/yellow] 未发现 '{company_name}' 关联的任何请求类型。")
    return combined_results


def parse_app_results(raw_data_list):
    """解析合并后的数据列表，生成指定表结构，包含新增的列。"""
    parsed_list = []
    if not raw_data_list: return parsed_list
    for item in raw_data_list:
        parsed_item = {
            '备案主体名': item.get('unitName', ''),
            '小程序': '',
            'app': '',
            '备案号': item.get('serviceLicence', ''),
            '单位性质': item.get('natureName', ''),
            '审核时间': item.get('updateRecordTime', '')
        }
        detected_type = item.get('detected_type')
        service_name = item.get('serviceName', '')
        if detected_type == 'mapp':
            parsed_item['小程序'] = service_name
        elif detected_type == 'app':
            parsed_item['app'] = service_name
        parsed_list.append(parsed_item)
    return parsed_list


def write_app_results_to_excel(output_dir, company_name, data):
    """将解析后的小程序/APP数据写入独立的Excel文件。"""
    if not data: return
    filename_suffix = generate_filename_suffix(company_name, "apps")
    excel_path = os.path.join(output_dir, f"app_results{filename_suffix}.xlsx")
    df = pd.DataFrame(data)
    try:
        df.to_excel(excel_path, index=False, sheet_name="App_MiniProgram_Data")
        logging.info(f"小程序/APP 查询结果已保存到: {excel_path}")
        cs_console.print(
            f"      [green]Success:[/green] 小程序/APP Excel已保存: '{os.path.basename(excel_path)}' ({len(df)} 条)")
    except Exception as e:
        logging.error(f"保存小程序/APP Excel失败: {e}")


def write_summary_app_report_to_excel(output_dir, target_name, all_data):
    """将单个查询目标下，所有公司的小程序/APP数据汇总并写入一个总的Excel文件。"""
    if not all_data: return
    filename_suffix = generate_filename_suffix(target_name, "app_summary")
    excel_path = os.path.join(output_dir, f"app_summary{filename_suffix}.xlsx")
    df = pd.DataFrame(all_data)
    try:
        df.to_excel(excel_path, index=False, sheet_name="All_Apps_Summary")
        logging.info(f"小程序/APP 汇总报告已保存到: {excel_path}")
        cs_console.print(
            f"    [green]Success:[/green] 小程序/APP汇总报告已保存: '{os.path.basename(excel_path)}' (共 {len(df)} 条)")
    except Exception as e:
        logging.error(f"保存小程序/APP汇总报告失败: {e}")


def write_final_summary_report(output_base_dir, all_data):
    """
    将所有查询目标的所有小程序/APP数据，汇总并写入一个最终的、位于根目录的总表中。
    """
    if not all_data:
        cs_console.print(f"\n  [yellow]INFO:[/yellow] 未发现任何小程序/APP数据，不生成最终汇总报告。")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    excel_path = os.path.join(output_base_dir, f"FINAL_APP_SUMMARY_{timestamp}.xlsx")

    df = pd.DataFrame(all_data)

    try:
        df.to_excel(excel_path, index=False, sheet_name="Final_All_Apps_Summary")
        logging.info(f"最终小程序/APP汇总报告已保存到: {excel_path}")
        cs_console.print(
            f"    [green]Success:[/green] 最终汇总报告已保存: '{os.path.basename(excel_path)}' (共 {len(df)} 条)")
    except Exception as e:
        logging.error(f"保存最终小程序/APP汇总报告失败: {e}")
        cs_console.print(
            f"      [bold red]Error:[/bold red] 保存最终小程序/APP汇总报告失败: {os.path.basename(excel_path)}")


def sanitize_sheet_name(name):
    """清理字符串，使其符合Excel工作表名称的规范。"""
    invalid_chars = r'\[]:\*?/\\';
    name_str = str(name) if name is not None else "Unknown"
    for c in invalid_chars: name_str = name_str.replace(c, '')
    return name_str[:31]


def generate_filename_suffix(company_name, stage=""):
    """根据公司名、当前阶段和时间戳生成唯一的文件名后缀。"""
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M")
    sanitized_company_name = sanitize_sheet_name(company_name)
    return f"_{sanitized_company_name}_{stage}_{timestamp_str}" if stage else f"_{sanitized_company_name}_{timestamp_str}"


def write_quake_results_to_excel(output_dir, company_name, data, stage=""):
    """(修正后) 将Quake查询的解析结果保存到指定的输出目录中。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    # 不再自己构建目录，直接使用传入的 output_dir
    excel_path = os.path.join(output_dir, f"quake_result{filename_suffix}.xlsx")
    df = pd.DataFrame(data)
    try:
        df.to_excel(excel_path, index=False, sheet_name="Quake_Data")
        logging.info(f"Quake 查询结果已保存到: {excel_path} (共 {len(df)} 条)")
        cs_console.print(
            f"    [green]Success:[/green] Quake Excel 已保存: '{os.path.basename(excel_path)}' ({len(df)} 条)")
    except Exception as e:
        logging.error(f"保存Quake Excel失败 ({excel_path}): {e}", exc_info=True)
        cs_console.print(f"    [bold red]Error:[/bold red] 保存Quake Excel失败: {os.path.basename(excel_path)}")


def write_fofa_results_to_excel(output_dir, target_name, data, stage="fofa_reverse_lookup"):
    """(修正后) 将Fofa反查的解析结果保存到指定的输出目录中。"""
    if not data: return
    filename_suffix = generate_filename_suffix(target_name, stage)
    # 直接使用传入的 output_dir
    excel_path = os.path.join(output_dir, f"fofa_results{filename_suffix}.xlsx")
    df = pd.DataFrame(data)
    try:
        df.to_excel(excel_path, index=False, sheet_name="Fofa_Reverse_Lookup")
        logging.info(f"Fofa 反查结果已保存到: {excel_path} (共 {len(df)} 条)")
        cs_console.print(
            f"    [green]Success:[/green] Fofa反查Excel已保存: '{os.path.basename(excel_path)}' ({len(df)} 条)")
    except Exception as e:
        logging.error(f"保存Fofa反查Excel失败 ({excel_path}): {e}", exc_info=True)
        cs_console.print(f"    [bold red]Error:[/bold red] 保存Fofa反查Excel失败: {os.path.basename(excel_path)}")


def write_ips_to_file(output_dir, company_name, ip_list, stage=""):
    """(修正后) 将IP地址列表保存到指定的输出目录中。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    # 直接使用传入的 output_dir
    ip_list_file = os.path.join(output_dir, f"ips{filename_suffix}.txt")

    # --- 后续的文件写入逻辑保持不变 ---
    valid_ips = set()
    for ip_addr in ip_list:
        if isinstance(ip_addr, str) and ip_addr.count('.') == 3:
            try:
                parts = ip_addr.split('.')
                if all(0 <= int(p) <= 255 for p in parts): valid_ips.add(ip_addr)
            except ValueError:
                logging.warning(f"向ip文件写入时，发现无效的IP格式: {ip_addr} (公司: {company_name})，已跳过。")
    if not valid_ips:
        return None
    sorted_ips = sorted(list(valid_ips));
    c_segments = defaultdict(int)
    for ip_val in sorted_ips:
        c_segments[ip_val.rsplit('.', 1)[0]] += 1
    try:
        with open(ip_list_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted_ips));
            c_segment_info_count = 0
            for segment, count in c_segments.items():
                if count > 1: f.write(f"\n[INFO] 可能存在 C 段: {segment}.0/24 ({count})"); c_segment_info_count += 1
            if c_segment_info_count > 0: f.write("\n")
        logging.info(f"IP 列表已保存到: {ip_list_file} (共 {len(sorted_ips)} 条)")
        cs_console.print(
            f"    [green]Success:[/green] IP列表已保存: '{os.path.basename(ip_list_file)}' ({len(sorted_ips)} 条)")
        if c_segment_info_count > 0:
            cs_console.print(f"      [blue]INFO:[/blue] 发现 {c_segment_info_count} 个可能C段 (详情见文件)")
    except Exception as e:
        logging.error(f"保存IP列表文件失败 ({ip_list_file}): {e}", exc_info=True)
        return None
    return ip_list_file


def write_urls_to_txt_file(output_dir, name_for_file, url_list, stage=""):
    """(修正后) 将URL列表保存到指定的输出目录中。"""
    filename_suffix = generate_filename_suffix(name_for_file, stage)
    # 直接使用传入的 output_dir
    url_list_file = os.path.join(output_dir, f"extracted_urls{filename_suffix}.txt")

    # --- 后续的文件写入逻辑保持不变 ---
    valid_urls = {str(u).strip() for u in url_list if u and str(u).strip()}
    if not valid_urls:
        return None
    sorted_urls = sorted(list(valid_urls))
    try:
        with open(url_list_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted_urls))
        logging.info(f"提取的 URL 列表已保存到: {url_list_file} (共 {len(sorted_urls)} 条)")
        cs_console.print(
            f"    [green]Success:[/green] URL列表已保存: '{os.path.basename(url_list_file)}' ({len(sorted_urls)} 条)")
    except Exception as e:
        logging.error(f"保存提取的URL列表文件失败 ({url_list_file}): {e}", exc_info=True)
        return None
    return url_list_file


def run_observer_ward(company_name, company_dir_path, urls_to_fingerprint, stage="", url_list_file_path=None):
    """运行observer_ward.exe进行URL指纹识别。根据全局 SHOW_SCAN_INFO 控制输出。
       如果提供了 url_list_file_path，则直接使用该文件作为输入。
       确保传递给工具的路径是绝对路径。
    """
    global SHOW_SCAN_INFO
    filename_suffix = generate_filename_suffix(company_name, stage)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_dir = os.path.join(script_dir, 'tools')
    observer_ward_path = os.path.join(tools_dir, 'observer_ward.exe')  # Absolute path to the tool

    # company_dir_path is passed from the calling function (e.g., run_basic_mode)
    # It might be relative if OUTPUT_BASE_DIR is relative. Convert to absolute.
    absolute_company_dir_path = os.path.abspath(company_dir_path)
    # Ensure the directory exists (though it should have been created by the caller, e.g. run_basic_mode)
    os.makedirs(absolute_company_dir_path, exist_ok=True)

    if not os.path.exists(observer_ward_path):
        logging.warning(f"observer_ward.exe 文件未找到 ({observer_ward_path})，跳过 URL 指纹识别。")
        cs_console.print(f"  [bold red]Error:[/bold red] observer_ward.exe 未找到，跳过指纹识别。")
        return

    observer_input_url_file_for_tool_abs = None  # Will store the absolute path to the input file

    if url_list_file_path and os.path.exists(url_list_file_path):
        # If an existing URL list file is provided, make its path absolute
        observer_input_url_file_for_tool_abs = os.path.abspath(url_list_file_path)
        logging.info(f"observer_ward 将使用已存在的URL列表文件 (绝对路径): {observer_input_url_file_for_tool_abs}")
    elif urls_to_fingerprint:
        valid_urls_for_observer = [str(u).strip() for u in urls_to_fingerprint if u and str(u).strip()]
        if not valid_urls_for_observer:
            logging.info(f"没有有效的 URL 传递给 observer_ward (公司: {company_name}, 阶段: {stage})")
            cs_console.print(f"    [yellow]INFO:[/yellow] 无有效URL，跳过指纹识别 ({stage})")
            return

        # Create the temporary input file in the absolute company directory path
        _observer_input_url_file_name = f"urls_for_observer{filename_suffix}.txt"
        observer_input_url_file_for_tool_abs = os.path.join(absolute_company_dir_path, _observer_input_url_file_name)
        try:
            with open(observer_input_url_file_for_tool_abs, 'w', encoding='utf-8') as f:
                for url_item in sorted(list(set(valid_urls_for_observer))): f.write(url_item + '\n')
            logging.info(
                f"为 observer_ward 动态创建的 URL 列表已保存 (绝对路径): {observer_input_url_file_for_tool_abs} ({len(set(valid_urls_for_observer))} 条)")
            cs_console.print(
                f"    [grey50]为指纹识别动态创建了URL列表: '{os.path.basename(observer_input_url_file_for_tool_abs)}'[/grey50]")
        except Exception as e:
            logging.error(f"保存 observer_ward 动态输入URL文件失败 ({observer_input_url_file_for_tool_abs}): {e}",
                          exc_info=True)
            cs_console.print(
                f"    [bold red]Error:[/bold red] 保存observer_ward动态输入URL文件失败: {os.path.basename(observer_input_url_file_for_tool_abs)}")
            return
    else:
        logging.info(f"没有URL列表或URL文件提供给 observer_ward (公司: {company_name}, 阶段: {stage})")
        cs_console.print(f"    [yellow]INFO:[/yellow] 无URL，跳过指纹识别 ({stage})")
        return

    # observer_ward's output CSV file path (absolute)
    _observer_ward_output_file_name = f"url_fingerprint{filename_suffix}.csv"
    observer_ward_output_file_abs = os.path.join(absolute_company_dir_path, _observer_ward_output_file_name)

    # Build observer_ward command using absolute paths for -l and -o
    observer_ward_command = [observer_ward_path, '-l', observer_input_url_file_for_tool_abs, '-o',
                             observer_ward_output_file_abs]

    if not SHOW_SCAN_INFO:
        observer_ward_command.append('--silent')

    logging.info(
        f"开始执行 observer_ward (公司: {company_name}, 阶段: {stage}) 命令: {' '.join(observer_ward_command)}")
    cs_console.print(f"    [blue]执行:[/blue] observer_ward URL指纹识别 ({stage})...")
    if SHOW_SCAN_INFO:
        cs_console.print(f"      [grey50](observer_ward 实时输出已开启...)[/grey50]")

    subprocess_kwargs = {"check": True, "cwd": tools_dir}  # Keep cwd=tools_dir if observer_ward needs it
    if not SHOW_SCAN_INFO:
        subprocess_kwargs["capture_output"] = True;
        subprocess_kwargs["text"] = True
        subprocess_kwargs["encoding"] = 'utf-8';
        subprocess_kwargs["errors"] = 'ignore'
    else:  # 如果显示实时输出，尝试用gbk解码终端（某些工具可能用此编码）
        subprocess_kwargs["encoding"] = 'gbk';
        subprocess_kwargs["errors"] = 'ignore'

    try:
        process = subprocess.run(observer_ward_command, **subprocess_kwargs)
        if not SHOW_SCAN_INFO and process:  # 记录捕获的输出
            logging.debug(f"observer_ward STDOUT: {process.stdout}")
            logging.debug(f"observer_ward STDERR: {process.stderr}")
        logging.info(
            f"observer_ward 执行成功 (公司: {company_name}), 输出: {observer_ward_output_file_abs}")  # Log absolute path
        cs_console.print(
            f"      [green]Success:[/green] 指纹识别结果已保存: '{os.path.basename(observer_ward_output_file_abs)}'")
    except subprocess.CalledProcessError as e:
        logging.error(f"observer_ward 执行失败 (公司: {company_name}): {e}", exc_info=True)
        if not SHOW_SCAN_INFO and hasattr(e, 'stdout') and e.stdout: logging.error(
            f"observer_ward STDOUT on error: {e.stdout}")
        if not SHOW_SCAN_INFO and hasattr(e, 'stderr') and e.stderr: logging.error(
            f"observer_ward STDERR on error: {e.stderr}")
        cs_console.print(f"      [bold red]Error:[/bold red] observer_ward 执行失败 (详情见日志)。")
    except FileNotFoundError:
        logging.error(f"observer_ward 执行时文件未找到错误 ({observer_ward_path}) (公司: {company_name})",
                      exc_info=True)
        cs_console.print(f"      [bold red]Error:[/bold red] observer_ward.exe 或其依赖文件未找到。")
    except Exception as e:
        logging.error(f"observer_ward 执行时发生未知错误 (公司: {company_name}): {e}", exc_info=True)
        cs_console.print(f"      [bold red]Error:[/bold red] observer_ward 执行时发生未知错误 (详情见日志)。")


def process_all_generated_csvs(output_base_dir_param):
    """
    (修正后) 将指定根目录下所有子目录中的CSV文件（无论嵌套多深）都转换为Excel。
    """
    cs_console.print(f"\n[green]INFO:[/green] 开始最终的CSV到Excel批量转换...")
    logging.info(f"开始最终的CSV到Excel批量转换处理，根目录: {output_base_dir_param}")

    processed_files_count = 0
    deleted_csv_count = 0
    error_files_count = 0

    if not os.path.exists(output_base_dir_param):
        logging.warning(f"输出根目录 {output_base_dir_param} 不存在，跳过CSV处理。")
        cs_console.print(f"  [yellow]Warning:[/yellow] 输出根目录 '{output_base_dir_param}' 不存在，跳过CSV处理。")
        return

    # --- 关键修改点：使用 os.walk()进行递归遍历 ---
    for dirpath, _, filenames in os.walk(output_base_dir_param):
        for filename in filenames:
            if filename.endswith(".csv"):
                csv_file_path = os.path.join(dirpath, filename)
                excel_file_name = os.path.splitext(filename)[0] + ".xlsx"
                excel_file_path = os.path.join(dirpath, excel_file_name)  # 在同一个目录下生成Excel

                logging.info(f"CSV处理 - 准备转换: {csv_file_path} -> {excel_file_path}")
                try:
                    df_csv = None
                    # --- 读取CSV的逻辑保持不变 ---
                    try:
                        df_csv = pd.read_csv(csv_file_path, encoding='utf-8-sig')
                    except UnicodeDecodeError:
                        try:
                            df_csv = pd.read_csv(csv_file_path, encoding='gbk')
                        except UnicodeDecodeError:
                            df_csv = pd.read_csv(csv_file_path, encoding='latin1')

                    if df_csv is None or df_csv.empty:
                        logging.warning(f"CSV文件为空或读取失败: {csv_file_path}，将尝试删除。")
                        try:
                            os.remove(csv_file_path)
                            deleted_csv_count += 1
                        except OSError as e_remove:
                            logging.error(f"删除空的CSV文件 {csv_file_path} 失败: {e_remove}")
                        continue

                    # --- 写入Excel的逻辑保持不变 ---
                    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                        df_csv.to_excel(writer, sheet_name="原始数据", index=False)
                        if 'status_code' in df_csv.columns:
                            df_copy = df_csv.copy()
                            df_copy['status_code'] = pd.to_numeric(df_copy['status_code'], errors='coerce').fillna(
                                0).astype(int)
                            valid_status_codes = [200, 301, 302]
                            df_valid = df_copy[df_copy['status_code'].isin(valid_status_codes)]
                            df_invalid = df_copy[~df_copy['status_code'].isin(valid_status_codes)]
                            if not df_valid.empty: df_valid.to_excel(writer, sheet_name="有效表", index=False)
                            if not df_invalid.empty: df_invalid.to_excel(writer, sheet_name="无效表", index=False)

                    processed_files_count += 1
                    try:
                        os.remove(csv_file_path)
                        deleted_csv_count += 1
                    except OSError as e_remove:
                        logging.error(f"删除已转换的CSV文件 {csv_file_path} 失败: {e_remove}")

                except Exception as e:
                    logging.error(f"处理CSV文件 {csv_file_path} 失败: {e}", exc_info=True)
                    cs_console.print(
                        f"  [bold red]Error:[/bold red] 处理CSV '{os.path.basename(csv_file_path)}' 失败 (详情见日志).")
                    error_files_count += 1

    cs_console.print(f"[green]INFO:[/green] CSV到Excel转换处理完成:")
    cs_console.print(f"  成功转换: {processed_files_count} 个文件")
    cs_console.print(f"  成功删除原始CSV: {deleted_csv_count} 个文件")
    if error_files_count > 0:
        cs_console.print(f"  [yellow]处理失败:[/yellow] {error_files_count} 个文件 (详情请查看日志)")


def move_txt_to_related_materials(company_dir_path, company_name_for_log):
    """将指定公司目录下的所有.txt文件移动到 'related_materials' 子目录。"""
    related_materials_dir = os.path.join(company_dir_path, "related_materials")
    os.makedirs(related_materials_dir, exist_ok=True);
    moved_txt_count = 0
    if os.path.exists(company_dir_path):
        for filename_txt in os.listdir(company_dir_path):
            if filename_txt.endswith(".txt"):
                source_path = os.path.join(company_dir_path, filename_txt)
                if os.path.isfile(source_path):  # Ensure it's a file, not a directory ending in .txt
                    destination_path = os.path.join(related_materials_dir, filename_txt)
                    try:
                        if os.path.exists(destination_path):
                            logging.warning(
                                f"目标文件已存在 '{destination_path}' (公司: {company_name_for_log})，将覆盖。")
                            os.remove(destination_path)
                        os.rename(source_path, destination_path);
                        logging.info(
                            f"已将文件 '{filename_txt}' 移动到 '{related_materials_dir}' (公司: {company_name_for_log})");
                        moved_txt_count += 1
                    except Exception as e:
                        logging.error(
                            f"移动文件 '{filename_txt}' 到 '{related_materials_dir}' (公司: {company_name_for_log}) 失败: {e}");
                        cs_console.print(
                            f"    [bold red]Error:[/bold red] 移动文件 '{filename_txt}' 失败 (公司: {company_name_for_log}, 详情见日志)")
        if moved_txt_count > 0: cs_console.print(
            f"    [green]Move:[/green] 已将 {moved_txt_count} 个 .txt 文件移动到 'related_materials' (公司: {company_name_for_log})。")
    else:
        logging.warning(
            f"公司目录 '{company_dir_path}' 未找到或创建失败 (公司: {company_name_for_log})，无法移动 .txt 文件。")


# ======================= Advanced Mode Specific Functions =======================
def write_ports_to_temp_file(port_list):
    """为fscan准备端口列表临时文件。"""
    valid_ports = set()
    for p in port_list:
        try:
            port_num = int(p)
            valid_ports.add(port_num) if 0 <= port_num <= 65535 else logging.warning(
                f"无效端口号 (范围): {p}")
        except (ValueError, TypeError):
            logging.warning(f"无效端口号 (非数字): {p}")
    all_ports = valid_ports | DEFAULT_PORTS
    if not all_ports: logging.warning("无有效端口写入临时文件。"); cs_console.print(
        "    [yellow]Warning:[/yellow] 无有效端口 (fscan将用默认)。"); return None
    sorted_ports = sorted(list(all_ports))
    try:
        # NamedTemporaryFile returns an absolute path by default for .name
        with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='company_ports_', suffix='.txt',
                                         encoding='utf-8') as tf:
            tf.write(','.join(map(str, sorted_ports)))
            temp_file_path = tf.name  # This is an absolute path
        logging.info(f"端口列表写入临时文件: {temp_file_path} ({len(sorted_ports)}个)")
        return temp_file_path
    except Exception as e:
        logging.error(f"写入端口临时文件失败: {e}", exc_info=True)
        cs_console.print(
            f"    [bold red]Error:[/bold red] 写入端口临时文件失败: {e}")
        return None


def fscan_start(company_name, iplist_file_path_param, port_file_path_param=None):
    """(已修正) 运行fscan进行端口和服务扫描。确保使用传入的路径。"""
    global SHOW_SCAN_INFO
    filename_suffix = generate_filename_suffix(company_name, "fscan")

    if not iplist_file_path_param or not os.path.exists(iplist_file_path_param):
        logging.error(f"fscan的IP列表文件无效或未找到: {iplist_file_path_param} ({company_name})")
        cs_console.print(
            f"    [bold red]Error:[/bold red] fscan IP列表文件无效: {os.path.basename(iplist_file_path_param or 'N/A')}")
        return None

    # <--- 核心修正点：从传入的文件路径推断正确的公司输出目录 ---
    absolute_iplist_file_path = os.path.abspath(iplist_file_path_param)
    absolute_company_dir_path = os.path.dirname(absolute_iplist_file_path)
    # 不再需要自己构建 company_dir_path_rel 或 makedirs

    absolute_output_file_path = os.path.join(absolute_company_dir_path, f"fscan_output{filename_suffix}.txt")
    absolute_port_file_path = os.path.abspath(port_file_path_param) if port_file_path_param and os.path.exists(
        port_file_path_param) else None

    if not absolute_port_file_path and port_file_path_param:
        logging.warning(f"指定端口文件 {port_file_path_param} 不存在，fscan将用内置端口。")
        cs_console.print(
            f"    [yellow]Warning:[/yellow] 端口文件 {os.path.basename(port_file_path_param)} 不存在，fscan用内置端口。")

    logging.info(
        f"开始fscan: {company_name}, IP列表: {absolute_iplist_file_path}, 端口列表: {absolute_port_file_path if absolute_port_file_path else '默认'}")
    cs_console.print(f"    [blue]执行:[/blue] fscan 端口与服务扫描...")
    if SHOW_SCAN_INFO: cs_console.print(f"      [grey50](fscan 实时输出已开启...)[/grey50]")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_dir = os.path.join(script_dir, 'tools')
    fscan_exe_path = os.path.join(tools_dir, 'fscan64.exe')

    if not os.path.exists(fscan_exe_path):
        logging.error(f"fscan64.exe 未找到: {fscan_exe_path}")
        cs_console.print(f"    [bold red]Error:[/bold red] fscan64.exe 未找到于 '{tools_dir}'.")
        return None

    command = [fscan_exe_path, '-hf', absolute_iplist_file_path, '-t', '1600', '-np', '-nopoc', '-nobr', '-o',
               absolute_output_file_path]
    if not SHOW_SCAN_INFO: command.append('-silent')
    if absolute_port_file_path: command.extend(['-portf', absolute_port_file_path])

    logging.info(f"执行 fscan 命令: {' '.join(command)} (工作目录: {tools_dir})")
    subprocess_kwargs_fscan = {"check": True, "cwd": tools_dir}
    if not SHOW_SCAN_INFO:
        subprocess_kwargs_fscan["capture_output"] = True
        subprocess_kwargs_fscan["text"] = True
        subprocess_kwargs_fscan["encoding"] = 'utf-8'
        subprocess_kwargs_fscan["errors"] = 'ignore'

    try:
        process = subprocess.run(command, **subprocess_kwargs_fscan)
        if not SHOW_SCAN_INFO and process: logging.debug(f"fscan STDOUT: {process.stdout}\nSTDERR: {process.stderr}")
        logging.info(f"fscan 执行成功，输出文件: {absolute_output_file_path}")
        cs_console.print(
            f"      [green]Success:[/green] fscan扫描完成, 结果: '{os.path.basename(absolute_output_file_path)}'")
    except subprocess.CalledProcessError as e:
        logging.error(f"fscan 执行失败: {e}", exc_info=True)
        if not SHOW_SCAN_INFO and hasattr(e, 'stdout') and e.stdout: logging.error(f"fscan STDOUT on error: {e.stdout}")
        if not SHOW_SCAN_INFO and hasattr(e, 'stderr') and e.stderr: logging.error(f"fscan STDERR on error: {e.stderr}")
        cs_console.print(f"      [bold red]Error:[/bold red] fscan 执行失败 (详情见日志)。")
        return None
    except FileNotFoundError:
        logging.error(f"fscan64.exe 执行出错 (FileNotFoundError)。", exc_info=True)
        cs_console.print(
            f"      [bold red]Error:[/bold red] fscan64.exe 执行出错 (FileNotFoundError)。")
        return None
    except Exception as e:
        logging.error(f"fscan 执行时发生未知错误: {e}", exc_info=True)
        cs_console.print(
            f"      [bold red]Error:[/bold red] fscan 未知错误 (详情见日志).")
        return None

    if absolute_output_file_path and (
            not os.path.exists(absolute_output_file_path) or os.path.getsize(absolute_output_file_path) == 0):
        logging.warning(f"fscan 输出文件为空或未生成: {absolute_output_file_path}")
        cs_console.print(
            f"      [yellow]Warning:[/yellow] fscan 输出文件为空或未生成: {os.path.basename(absolute_output_file_path or 'N/A')}")
    return absolute_output_file_path


class FscanBeautify:
    """处理和美化fscan原始输出的类。"""

    def __init__(self, file, company_name):
        self.p = ['存活IP段', '开放端口', '系统', 'Exp', 'Poc', '网站标题', '弱口令', '指纹']
        self.AliveIp, self.OpenPort, self.OsList, self.ExpList, self.PocList, self.TitleList, self.WeakPasswd, self.Finger = (
            [] for _ in range(8))
        self.filePath: str = file;  # This should be an absolute path from fscan_start
        self.company_name: str = company_name;
        self.fscan_output_urls = set()

    def readFile(self):
        """逐行读取fscan输出文件，进行初步清理。"""
        try:
            with open(self.filePath, "r", encoding="utf-8", errors='ignore') as f:
                for i in f.readlines(): yield i.strip("\n").replace('\x1b[36m', "").replace('\x1b[0m', "")
        except FileNotFoundError:
            logging.error(f"FscanBeautify: fscan输出文件未找到 {self.filePath}");
            cs_console.print(
                f"    [bold red]Error (FscanBeautify):[/bold red] fscan输出文件未找到 {os.path.basename(self.filePath or 'N/A')}");
            return iter(
                [])

    def parserData(self):
        """使用正则表达式解析fscan的每一行输出，分类存储数据。"""
        for data in self.readFile():
            OpenPort = "".join(re.findall(r'^\d\S+', data))
            if OpenPort: self.OpenPort.append({"IP": OpenPort.split(":")[0], "Port": OpenPort.split(":")[-1]})
            AliveIp = "".join(re.findall(r"\[\*]\sLiveTop\s\d+\.\d+\.\d+\.\d+/\d+.*", data))
            if AliveIp: self.AliveIp.append({"Cidr": "".join(re.findall(r"\d+\.\d+\.\d+\.\d+/\d+", AliveIp)),
                                             "Count": int("".join(re.findall(r"\d+$", AliveIp)))})
            OsList_match = re.search(r"\[\*]\s(\d+\.\d+\.\d+\.\d+)\s+(.*)", data)
            if OsList_match and "LiveTop" not in data and "WebTitle" not in data and "InfoScan" not in data:
                ip, oss = OsList_match.group(1), OsList_match.group(2).strip().replace(OsList_match.group(1),
                                                                                       "").replace("[*]", "").strip()
                if oss: self.OsList.append({"IP": ip, "OS": oss})
            ExpList_data_match = re.match(r"\[\+]\s(\d+\.\d+\.\d+\.\d+:\d+)\s+(.+)", data)
            if ExpList_data_match and "PocScan" not in data and not data.startswith("[+] http"):
                self.ExpList.append(
                    {"IP:Port": ExpList_data_match.group(1), "Exp": ExpList_data_match.group(2).strip()})
            PocList_match = re.search(r"\[\+]\s(?:PocScan\s)?(https?://\S+)\s+(.+)", data)
            if PocList_match:
                url = PocList_match.group(1)
                self.PocList.append({"Url": url, "Poc": PocList_match.group(2).strip()});
                self.fscan_output_urls.add(url)
            TitleList_data_match = re.search(r'\[\*]\sWebTitle.*(https?://\S+).*code:(\d+).*len:(\d+).*title:(.*)',
                                             data)
            if TitleList_data_match:
                url = TitleList_data_match.group(1)
                self.TitleList.append({"Url": url, "StatusCode": int(TitleList_data_match.group(2)),
                                       "Length": int(TitleList_data_match.group(3)),
                                       "Title": TitleList_data_match.group(4).strip()})
                self.fscan_output_urls.add(url)
            WeakPasswd_data = re.findall(r'((ftp|mysql|mssql|SMB|RDP|Postgres|SSH|oracle|SMB2-shares)(:|\s).*)', data,
                                         re.I)
            if WeakPasswd_data and "title" not in data and '[->]' not in data:
                parts = WeakPasswd_data[0][0].split(":")
                passwd = parts[3] if len(parts) > 3 else (parts[2] if len(parts) == 3 else '')
                protocol = parts[0].split(" ")[0] if " " in parts[0] else parts[0]
                port_str = parts[2]  # This logic was a bit complex, check if port finding is robust
                port_val_str = "0"
                ip_val = "N/A"

                # Attempt to extract IP first
                ip_match = re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", WeakPasswd_data[0][0])
                if ip_match:
                    ip_val = ip_match.group(1)

                # Attempt to extract port
                # Example: "RDP dc2.vuln.com:3389 WORKGROUP\Administrator P@SSW0RD!" -> port 3389
                # Example: "SSH 192.168.1.1:22 root password" -> port 22
                port_search_match = re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)",
                                              WeakPasswd_data[0][0])  # IP:Port
                if not port_search_match:  # If not IP:Port, try protocol<space>host:port
                    port_search_match = re.search(r"\S+\s+[^:]+:(\d+)", WeakPasswd_data[0][0])
                if not port_search_match and len(parts) > 1:  # Fallback to original logic if simpler pattern fails
                    # Original logic for parts[1] or parts[2] being port might be service specific
                    # This part is tricky without more examples of fscan output for all protocols
                    # For now, if specific patterns fail, rely on a general search or a default.
                    port_from_general_search = re.search(r":(\d+)", WeakPasswd_data[0][0])
                    if port_from_general_search:
                        port_val_str = port_from_general_search.group(1)

                if port_search_match:
                    port_val_str = port_search_match.group(
                        port_search_match.lastindex)  # Get the last captured group (the port number)

                try:
                    port = int(port_val_str)
                except ValueError:
                    port = 0  # Default if parsing fails

                self.WeakPasswd.append(
                    {"Protocol": protocol, "IP": ip_val, "Port": port, "User&Passwd": passwd.strip(), "Info": ''})

            WeakPasswd_redis_mongo = re.findall(r'((redis|Mongodb)(:|\s).*)', data, re.I)
            if WeakPasswd_redis_mongo:
                all_parts = WeakPasswd_redis_mongo[0][0].split(" ")
                protocol = WeakPasswd_redis_mongo[0][1]
                ip_addr = "".join(re.findall(r"\d+\.\d+\.\d+\.\d+", all_parts[0]))
                port_str_match = re.search(r":(\d+)", all_parts[0])
                port_val = int(port_str_match.group(1)) if port_str_match else 0
                passwd, info = "", ""
                if protocol.lower() == 'redis':
                    if len(all_parts) > 1 and all_parts[1] == "Unauthorized":
                        info = "Unauthorized"
                    elif len(all_parts) > 2 and all_parts[1] == "Password":
                        passwd = all_parts[2]
                    elif len(all_parts) > 1:
                        info = " ".join(all_parts[1:])
                elif protocol.lower() == 'mongodb':
                    if len(all_parts) > 1 and "unauthorized" in " ".join(all_parts[1:]).lower():
                        info = "Unauthorized"
                    elif len(all_parts) > 1:
                        info = " ".join(all_parts[1:])
                self.WeakPasswd.append(
                    {"Protocol": protocol, "IP": ip_addr, "Port": port_val, "User&Passwd": passwd.strip(),
                     "Info": info.strip()})
            WeakPasswd_mem = re.findall(r"((Memcached)(:|\s).*)", data, re.I)
            if WeakPasswd_mem:
                mc_all = WeakPasswd_mem[0][0].split(" ")
                passwd = mc_all[2] if len(mc_all) > 2 else ""
                protocol = mc_all[0]
                port_mem = (mc_all[1].split(":"))[-1] if len(mc_all) > 1 and ":" in mc_all[1] else "0"
                ip_mem = "".join(re.findall(r"\d+\.\d+\.\d+\.\d+", WeakPasswd_mem[0][0]))
                self.WeakPasswd.append(
                    {"Protocol": protocol, "IP": ip_mem, "Port": int(port_mem), "User&Passwd": passwd.strip(),
                     "Info": ''})
            Finger_data_match = re.search(r'.*InfoScan.*(https?://\S+)\s*(.*)', data)
            if Finger_data_match:
                url, finger_info_raw = Finger_data_match.group(1), Finger_data_match.group(2).strip()
                finger_info = finger_info_raw[1:-1] if finger_info_raw.startswith('[') and finger_info_raw.endswith(
                    ']') else finger_info_raw
                self.Finger.append({"Url": url, "Finger": finger_info});
                self.fscan_output_urls.add(url)

    def saveFile(self, output_dir_param, stage=""):
        """(已修正) 将解析后的fscan数据保存到Excel文件的不同工作表中。"""
        filename_suffix = generate_filename_suffix(self.company_name, stage)

        # <--- 核心修正点：直接使用从主流程传递来的 output_dir_param ---
        if not output_dir_param:
            logging.error(f"FscanBeautify.saveFile 未收到有效的输出目录参数。")
            return None
        absolute_company_dir = os.path.abspath(output_dir_param)
        os.makedirs(absolute_company_dir, exist_ok=True)
        # 不再自己通过 OUTPUT_BASE_DIR 和 company_name 构建路径

        fileName = os.path.join(absolute_company_dir, f"fscan_result{filename_suffix}.xlsx")

        def format_file_size(size_bytes):
            if size_bytes == 0: return "0 B"
            size_name = ("B", "KB", "MB", "GB", "TB")
            if abs(size_bytes) < 1:
                i = 0
            else:
                try:
                    i = int(math.log(abs(size_bytes), 1024))
                except ValueError:
                    i = 0
            if i >= len(size_name):
                i = len(size_name) - 1
            elif i < 0:
                i = 0
            p = pow(1024, i)
            s = round(size_bytes / p, 2)
            return f"{s} {size_name[i]}"

        try:
            logging.debug(f"尝试写入 fscan Excel: {fileName}")
            with pd.ExcelWriter(fileName) as writer:
                for index, s_data in enumerate(
                        [self.AliveIp, self.OpenPort, self.OsList, self.ExpList, self.PocList, self.TitleList,
                         self.WeakPasswd, self.Finger]):
                    if s_data: pd.DataFrame(s_data).to_excel(writer, sheet_name=self.p[index], index=False)
            logging.info(f"Pandas ExcelWriter 成功完成 {fileName}")
            if os.path.exists(fileName) and os.path.getsize(fileName) > 0:
                file_size_str = format_file_size(os.path.getsize(fileName))
                logging.info(f"fscan 结果已美化并保存到: {fileName} 大小: {file_size_str}")
                cs_console.print(
                    f"      [green]Success:[/green] fscan结果Excel: '{os.path.basename(fileName)}' ({file_size_str})")
                return fileName
            else:
                logging.error(f"Fscan Excel ({fileName}) 写入似乎完成但文件不存在或为空。")
                cs_console.print(
                    f"      [bold red]Error:[/bold red] fscan结果Excel保存后检查失败 (文件不存在或为空): {os.path.basename(fileName)}")
                return None
        except Exception as e:
            logging.error(f"保存美化后的fscan结果Excel或格式化大小时失败 ({fileName}): {e}", exc_info=True)
            cs_console.print(
                f"      [bold red]Error:[/bold red] 保存fscan结果Excel或格式化大小时失败: {os.path.basename(fileName)} (详情见日志)")
            if os.path.exists(fileName): logging.warning(
                f"Fscan Excel ({fileName}) 可能由于错误而部分创建或格式化大小失败。")
            return None

    def showInfo(self, stage=""):
        """使用rich.Table在控制台显示fscan结果的摘要信息。"""
        table = Table(box=box.ASCII2, header_style="yellow", title_style="red")
        table.add_column("项目", justify="center", style="red");
        table.add_column("个数", style="magenta", justify="center")
        has_info = False
        for index, s_data in enumerate(
                [self.AliveIp, self.OpenPort, self.OsList, self.ExpList, self.PocList, self.TitleList, self.WeakPasswd,
                 self.Finger]):
            if s_data: table.add_row(self.p[index], str(len(s_data))); has_info = True
        if has_info:
            cs_console.print(
                f"\n[bold blue]Fscan 结果摘要 ({self.company_name} - {stage}):[/bold blue]");
            cs_console.print(table)
        else:
            cs_console.print(
                f"    [yellow]INFO (Fscan):[/yellow] 公司 {self.company_name} ({stage}) 未发现可供展示的fscan扫描结果。")

    def run(self, output_dir_param, stage=""):  # output_dir_param is passed but not directly used for path here
        """执行FscanBeautify的主要流程：解析数据、显示信息、保存文件。"""
        if not self.filePath or not os.path.exists(self.filePath): logging.error(
            f"FscanBeautify: fscan原始文件无效: {self.filePath}"); cs_console.print(
            f"    [bold red]Error (FscanBeautify):[/bold red] fscan原始文件无效: {os.path.basename(self.filePath or 'N/A')}"); return None
        if os.path.getsize(self.filePath) == 0: logging.warning(
            f"FscanBeautify: fscan原始文件为空: {self.filePath}"); cs_console.print(
            f"    [yellow]Warning (FscanBeautify):[/yellow] fscan原始文件为空: {os.path.basename(self.filePath)}"); return None
        logging.info(f"处理fscan输出: {self.filePath}");
        cs_console.print(f"    [blue]处理:[/blue] fscan原始输出 '{os.path.basename(self.filePath)}'...")
        self.parserData();
        self.showInfo(stage)
        return self.saveFile(output_dir_param, stage)  # saveFile now handles its own pathing

    def get_fscan_urls(self):
        return self.fscan_output_urls


def process_fscan_output_advanced(file_path, company_dir_path, company_name, stage=""):
    """高级模式下处理fscan输出的包装函数。"""
    # file_path from fscan_start should be absolute now
    if not file_path or not os.path.exists(file_path): logging.error(
        f"fscan输出文件无效: {file_path}"); cs_console.print(
        f"    [bold red]Error:[/bold red] fscan输出文件无效: {os.path.basename(file_path or 'N/A')}"); return set(), None
    if os.path.getsize(file_path) == 0: logging.warning(f"fscan输出文件为空: {file_path}"); cs_console.print(
        f"    [yellow]Warning:[/yellow] fscan输出文件为空: {os.path.basename(file_path)}"); return set(), None

    beautify = FscanBeautify(file_path, company_name);
    # company_dir_path is passed to beautify.run, but beautify.saveFile now constructs its path using OUTPUT_BASE_DIR
    excel_file_name = beautify.run(company_dir_path, stage)
    return beautify.get_fscan_urls(), excel_file_name


# ======================= Basic Mode Main Logic (基础模式主逻辑) =======================
def run_basic_mode(db_conn, skip_fofa_fingerprint=False, no_fofa=False, types_to_check=None):
    if types_to_check is None:
        types_to_check = []
    start_time_basic = time.time()
    cs_console.print(f"[bold blue]基础模式启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    failed_targets = []
    # 1. 初始化总列表，用于收集所有目标的APP数据
    grand_total_apps_list = []

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

        parsed_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_quake_data is None:
            parsed_quake_data = query_all_pages(target_name, db_conn)

        if parsed_quake_data is None:
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败'})
            continue
        if not parsed_quake_data:
            failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'})
            continue

        cursor = db_conn.cursor()
        cursor.execute("SELECT target_id FROM Targets WHERE target_name = ?", (target_name,))
        current_target_id = cursor.fetchone()[0]

        target_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(target_name))
        os.makedirs(target_dir, exist_ok=True)
        cs_console.print(f"  [green]INFO:[/green] 已创建/确认总输出目录: '{target_dir}'")

        assets_from_this_target = defaultdict(lambda: {"ips": set(), "urls": set()})
        for item in parsed_quake_data:
            company_name_key = item.get("主体单位") or "未知主体单位"
            if item.get("IP"): assets_from_this_target[company_name_key]["ips"].add(item.get("IP"))
            if item.get("URL"): assets_from_this_target[company_name_key]["urls"].add(item.get("URL"))

        cs_console.print(
            f"  [green]Quake数据处理完成:[/green] 发现 {len(assets_from_this_target)} 个主体单位。")

        all_parsed_apps_for_target = []

        for company_name, assets in assets_from_this_target.items():
            company_sub_dir = os.path.join(target_dir, sanitize_sheet_name(company_name))
            os.makedirs(company_sub_dir, exist_ok=True)

            company_specific_parsed_data = [pd for pd in parsed_quake_data if
                                            (pd.get("主体单位") or "未知主体单位") == company_name]
            write_quake_results_to_excel(company_sub_dir, company_name, company_specific_parsed_data, stage="quake")
            if assets["ips"]: write_ips_to_file(company_sub_dir, company_name, list(assets["ips"]),
                                                stage="quake_ips")

            http_urls_from_quake = [url for url in assets["urls"] if
                                    url.lower().startswith(('http://', 'https://'))]
            if http_urls_from_quake:
                run_observer_ward(company_name, company_sub_dir, http_urls_from_quake,
                                  stage="fingerprint_from_quake")

            move_txt_to_related_materials(company_sub_dir, company_name)

            if types_to_check:
                if company_name and "未知主体" not in company_name:
                    raw_app_data = query_apps_and_miniprograms(company_name, db_conn, types_to_check)
                    if raw_app_data:
                        parsed_app_data = parse_app_results(raw_app_data)
                        write_app_results_to_excel(company_sub_dir, company_name, parsed_app_data)
                        all_parsed_apps_for_target.extend(parsed_app_data)

        if types_to_check and all_parsed_apps_for_target:
            cs_console.print(f"\n  [blue]汇总报告:[/blue] 正在为目标 '{target_name}' 生成小程序/APP汇总表格...")
            write_summary_app_report_to_excel(target_dir, target_name, all_parsed_apps_for_target)
            # 2. 将当前目标的结果追加到总列表中
            grand_total_apps_list.extend(all_parsed_apps_for_target)

        if not no_fofa:
            cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ?", (current_target_id,))
            raw_json_rows = cursor.fetchall()
            raw_quake_data_list = [json.loads(row[0]) for row in raw_json_rows]
            shared_ips = identify_shared_service_ips(raw_quake_data_list)
            if shared_ips:
                cs_console.print(
                    f"\n  [blue]INFO:[/blue] 根据规则识别出 {len(shared_ips)} 个共享服务IP，将从Fofa反查中排除。")
            all_ips_from_this_target = {ip for assets in assets_from_this_target.values() for ip in assets["ips"]}
            ips_for_fofa = all_ips_from_this_target - shared_ips
            if ips_for_fofa:
                cs_console.print(
                    f"\n  [blue]INFO:[/blue] 准备对目标 '{target_name}' 的 {len(ips_for_fofa)} 个过滤后IP进行Fofa反查...")
                fofa_parsed_data = check_and_get_fofa_cache(current_target_id, db_conn)
                if fofa_parsed_data is None:
                    cs_console.print(
                        f"    [blue]Fofa API查询:[/blue] 目标 '{target_name}'，开始通过Fofa API进行IP反查...")
                    fofa_raw_data, _ = query_fofa_by_ips(list(ips_for_fofa), current_target_id, db_conn)
                    fofa_parsed_data = parse_fofa_results(fofa_raw_data) if fofa_raw_data else []
                if fofa_parsed_data:
                    fofa_output_dir = os.path.join(target_dir, "fofa_results")
                    os.makedirs(fofa_output_dir, exist_ok=True)
                    write_fofa_results_to_excel(fofa_output_dir, target_name, fofa_parsed_data)
                    http_urls_from_fofa = [item["URL"] for item in fofa_parsed_data if
                                           item.get("URL", "").lower().startswith(('http://', 'https://'))]
                    if http_urls_from_fofa:
                        cs_console.print(
                            f"    [blue]INFO:[/blue] 从Fofa找到 {len(http_urls_from_fofa)} 个Web URL，进行独立指纹识别。")
                        if not skip_fofa_fingerprint:
                            run_observer_ward(target_name, fofa_output_dir, http_urls_from_fofa,
                                              stage="fingerprint_from_fofa")
                        else:
                            cs_console.print(f"      [yellow]跳过:[/yellow] 已根据参数跳过对Fofa结果的指纹识别。")
            else:
                cs_console.print(f"\n  [yellow]INFO:[/yellow] 过滤后，没有需要进行Fofa反查的独立IP。")
        else:
            cs_console.print(f"\n  [yellow]跳过:[/yellow] 已根据参数 (--no-fofa) 跳过Fofa IP反查流程。")

    cs_console.print(f"\n[bold blue]所有目标处理完毕，开始最终报告生成...[/bold blue]")

    # 3. 在所有目标处理完毕后，生成最终的汇总报告
    if types_to_check and grand_total_apps_list:
        cs_console.print(f"\n[bold blue]最终汇总报告:[/bold blue] 正在生成所有目标的APP/小程序最终汇总表格...")
        write_final_summary_report(OUTPUT_BASE_DIR, grand_total_apps_list)

    process_all_generated_csvs(OUTPUT_BASE_DIR)
    create_self_check_report(failed_targets, db_conn, "basic")
    end_time_basic = time.time()
    cs_console.print(
        f"\n[bold green]基础模式结束.[/bold green] 总耗时: {round(end_time_basic - start_time_basic, 2)} 秒.")


# ======================= Advanced Mode Main Logic (高级模式主逻辑) =======================
def run_advanced_mode(db_conn, skip_fofa_fingerprint=False, no_fofa=False, types_to_check=None):
    if types_to_check is None:
        types_to_check = []
    start_time_advanced = time.time()
    cs_console.print(f"[bold blue]高级模式启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return
    failed_targets = []
    # 1. 初始化总列表，用于收集所有目标的APP数据
    grand_total_apps_list = []

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")
        parsed_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_quake_data is None: parsed_quake_data = query_all_pages(target_name, db_conn)
        if parsed_quake_data is None:
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败'});
            continue
        if not parsed_quake_data:
            failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'});
            continue

        cursor = db_conn.cursor()
        cursor.execute("SELECT target_id FROM Targets WHERE target_name = ?", (target_name,))
        current_target_id = cursor.fetchone()[0]

        target_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(target_name))
        os.makedirs(target_dir, exist_ok=True)
        cs_console.print(f"  [green]INFO:[/green] 已创建/确认总输出目录: '{target_dir}'")

        assets_from_quake = defaultdict(lambda: {"ips": set(), "urls": set(), "allPort": set()})
        for item in parsed_quake_data:
            company_name_key = item.get("主体单位") or "未知主体单位_Advanced"
            if item.get("IP"): assets_from_quake[company_name_key]["ips"].add(item.get("IP"))
            if item.get("URL"): assets_from_quake[company_name_key]["urls"].add(item.get("URL"))
            if item.get("Port"): assets_from_quake[company_name_key]["allPort"].add(str(item.get("Port")))

        cs_console.print(f"  [green]Quake数据处理完成:[/green] 发现 {len(assets_from_quake)} 个主体单位。")
        all_parsed_apps_for_target = []

        company_processed_count_adv = 0
        for company_name, assets in assets_from_quake.items():
            company_processed_count_adv += 1
            cs_console.print(
                f"\n  ({company_processed_count_adv}/{len(assets_from_quake)}) 处理主体单位: [cyan]{company_name}[/cyan]")
            company_dir = os.path.join(target_dir, sanitize_sheet_name(company_name))
            os.makedirs(company_dir, exist_ok=True)
            company_specific_parsed_data = [pd_item for pd_item in parsed_quake_data if
                                            (pd_item.get("主体单位") or "未知主体单位_Advanced") == company_name]
            write_quake_results_to_excel(company_dir, company_name, company_specific_parsed_data, stage="quake")
            company_ips_list = list(assets["ips"])
            if company_ips_list:
                fscan_ip_list_file = os.path.join(company_dir, "ips_for_fscan.txt")
                with open(fscan_ip_list_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(sorted(company_ips_list)))
                write_ips_to_file(company_dir, company_name, company_ips_list, stage="quake_ips")
                http_urls_from_quake = [url for url in assets["urls"] if
                                        url.lower().startswith(('http://', 'https://'))]
                if http_urls_from_quake:
                    run_observer_ward(company_name, company_dir, http_urls_from_quake, stage="fingerprint_from_quake")
                cs_console.print(
                    f"    [blue]Fscan准备:[/blue] 将对 '{company_name}' 的 {len(company_ips_list)} 个IP进行扫描。")
                fscan_port_file = write_ports_to_temp_file(list(assets["allPort"]))
                fscan_output_file = fscan_start(company_name, fscan_ip_list_file, fscan_port_file)
                if fscan_output_file and os.path.exists(fscan_output_file) and os.path.getsize(fscan_output_file) > 0:
                    fscan_discovered_urls, _ = process_fscan_output_advanced(fscan_output_file, company_dir,
                                                                             company_name, "fscan_processed")
                    http_urls_from_fscan = [url for url in fscan_discovered_urls if
                                            url.lower().startswith(('http://', 'https://'))]
                    if http_urls_from_fscan:
                        cs_console.print(
                            f"      [blue]INFO:[/blue] 从fscan找到 {len(http_urls_from_fscan)} 个新Web URL，进行二次指纹识别。")
                        run_observer_ward(company_name, company_dir, http_urls_from_fscan, "fingerprint_from_fscan")
                if fscan_port_file and os.path.exists(fscan_port_file):
                    try:
                        os.remove(fscan_port_file)
                    except OSError as e:
                        logging.warning(f"删除fscan临时端口文件失败: {e}")
            move_txt_to_related_materials(company_dir, company_name)
            if types_to_check:
                if company_name and "未知主体" not in company_name:
                    raw_app_data = query_apps_and_miniprograms(company_name, db_conn, types_to_check)
                    if raw_app_data:
                        parsed_app_data = parse_app_results(raw_app_data)
                        write_app_results_to_excel(company_dir, company_name, parsed_app_data)
                        all_parsed_apps_for_target.extend(parsed_app_data)

        if types_to_check and all_parsed_apps_for_target:
            cs_console.print(f"\n  [blue]汇总报告:[/blue] 正在为目标 '{target_name}' 生成小程序/APP汇总表格...")
            write_summary_app_report_to_excel(target_dir, target_name, all_parsed_apps_for_target)
            # 2. 将当前目标的结果追加到总列表中
            grand_total_apps_list.extend(all_parsed_apps_for_target)

        if not no_fofa:
            cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ?", (current_target_id,))
            raw_json_rows = cursor.fetchall()
            raw_quake_data_list = [json.loads(row[0]) for row in raw_json_rows]
            shared_ips = identify_shared_service_ips(raw_quake_data_list)
            if shared_ips:
                cs_console.print(
                    f"\n  [blue]INFO:[/blue] 根据规则识别出 {len(shared_ips)} 个共享服务IP，将从Fofa反查中排除。")
            all_ips_from_this_target = {ip for assets in assets_from_quake.values() for ip in assets["ips"]}
            ips_for_fofa = all_ips_from_this_target - shared_ips
            if ips_for_fofa:
                cs_console.print(
                    f"\n  [blue]INFO:[/blue] 准备对目标 '{target_name}' 的 {len(ips_for_fofa)} 个过滤后IP进行Fofa反查...")
                fofa_parsed_data = check_and_get_fofa_cache(current_target_id, db_conn)
                if fofa_parsed_data is None:
                    cs_console.print(
                        f"    [blue]Fofa API查询:[/blue] 目标 '{target_name}'，开始通过Fofa API进行IP反查...")
                    fofa_raw_data, _ = query_fofa_by_ips(list(ips_for_fofa), current_target_id, db_conn)
                    fofa_parsed_data = parse_fofa_results(fofa_raw_data) if fofa_raw_data else []
                if fofa_parsed_data:
                    fofa_output_dir = os.path.join(target_dir, "fofa_results")
                    os.makedirs(fofa_output_dir, exist_ok=True)
                    write_fofa_results_to_excel(fofa_output_dir, target_name, fofa_parsed_data)
                    http_urls_from_fofa = [item["URL"] for item in fofa_parsed_data if
                                           item.get("URL", "").lower().startswith(('http://', 'https://'))]
                    if http_urls_from_fofa:
                        cs_console.print(
                            f"    [blue]INFO:[/blue] 从Fofa找到 {len(http_urls_from_fofa)} 个Web URL，进行独立指纹识别。")
                        if not skip_fofa_fingerprint:
                            run_observer_ward(target_name, fofa_output_dir, http_urls_from_fofa,
                                              stage="fingerprint_from_fofa")
                        else:
                            cs_console.print(f"      [yellow]跳过:[/yellow] 已根据参数跳过对Fofa结果的指纹识别。")
            else:
                cs_console.print(f"\n  [yellow]INFO:[/yellow] 过滤后，没有需要进行Fofa反查的独立IP。")
        else:
            cs_console.print(f"\n  [yellow]跳过:[/yellow] 已根据参数 (--no-fofa) 跳过Fofa IP反查流程。")

    cs_console.print(f"\n[bold blue]所有目标处理完毕，开始最终报告生成...[/bold blue]")

    # 3. 在所有目标处理完毕后，生成最终的汇总报告
    if types_to_check and grand_total_apps_list:
        cs_console.print(f"\n[bold blue]最终汇总报告:[/bold blue] 正在生成所有目标的APP/小程序最终汇总表格...")
        write_final_summary_report(OUTPUT_BASE_DIR, grand_total_apps_list)

    process_all_generated_csvs(OUTPUT_BASE_DIR)
    create_self_check_report(failed_targets, db_conn, "advanced")
    end_time_advanced = time.time()
    cs_console.print(
        f"\n[bold green]高级模式结束.[/bold green] 总耗时: {round(end_time_advanced - start_time_advanced, 2)} 秒.")


# ======================= Main Execution Logic (主执行逻辑) =======================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ICP Asset Express - 基于Quake和外部工具的信息收集与扫描脚本",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=f"""
    使用示例:
      python {os.path.basename(__file__)} -a -i my_targets.txt -o ./my_scan_results
      python {os.path.basename(__file__)} --basic --apikey YOUR_KEY --checkother app,mapp
      python {os.path.basename(__file__)} --advanced --no-fofa
      python {os.path.basename(__file__)} -a -i targets.txt -checkother app

    默认行为:
      如果不指定模式 (-a 或 -b)，脚本将默认以高级模式运行。
      默认不查询小程序或APP，需要通过 -checkother 参数指定。
    """
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('-b', '--basic', action='store_true', help="运行基础模式")
    mode_group.add_argument('-a', '--advanced', action='store_true', help="运行高级模式 (默认)")

    parser.add_argument('-i', '--input', type=str, help=f"指定输入文件名。默认为: '{INPUT_FILE}'。")
    parser.add_argument('-o', '--output', type=str, help="指定自定义的输出根目录。")
    parser.add_argument('--apikey', type=str, help="指定360 Quake API Key。")
    parser.add_argument('--showScanInfo', action='store_true', help="显示外部扫描工具的实时运行输出。")
    parser.add_argument('--skip-fofa-fingerprint', action='store_true', help="跳过对Fofa反查结果的URL进行指纹识别。")
    parser.add_argument('--no-fofa', action='store_true', help="完全跳过Fofa IP反查流程。")
    parser.add_argument('-checkother', type=str, help="查询额外信息，多个用逗号分隔 (app,mapp)。")

    args = parser.parse_args()

    # --- 全局变量根据命令行参数更新 ---
    if args.showScanInfo: SHOW_SCAN_INFO = True
    if args.input: INPUT_FILE = args.input
    if args.apikey: API_KEY = args.apikey

    SKIP_FOFA_FINGERPRINT = args.skip_fofa_fingerprint
    NO_FOFA = args.no_fofa

    types_to_check = []
    if args.checkother:
        types_to_check = [t.strip().lower() for t in args.checkother.split(',')]
        cs_console.print(f"[green]INFO:[/] 已设置查询额外信息: {types_to_check}")

    # --- 启动流程 ---
    script_start_time = time.time()
    LOG_FILE_PATH = ""
    db_conn = initialize_database()
    if not db_conn:
        cs_console.print("[bold red]CRITICAL:[/bold red] 无法连接到数据库，脚本将退出。")
        exit(1)

    cs_console.print(f"[green]INFO:[/green] 数据库 '{DB_FILE}' 初始化/连接成功。")

    chosen_mode_function = None
    if args.basic:
        cs_console.print("[bold underline green]启动基础模式 (Basic Mode)[/bold underline green]")
        LOG_FILE_PATH = "log_icp_basic.txt"
        OUTPUT_BASE_DIR = args.output if args.output else "results_icp_basic"
        chosen_mode_function = run_basic_mode
    else:
        cs_console.print("[bold underline green]启动高级模式 (Advanced Mode)[/bold underline green]")
        LOG_FILE_PATH = "log_icp_advanced.txt"
        OUTPUT_BASE_DIR = args.output if args.output else "results_icp_advanced"
        chosen_mode_function = run_advanced_mode

    configure_logging(LOG_FILE_PATH)
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    if chosen_mode_function:
        chosen_mode_function(db_conn, SKIP_FOFA_FINGERPRINT, NO_FOFA, types_to_check)

    # --- 清理工作 ---
    script_end_time = time.time()
    overall_duration = script_end_time - script_start_time
    current_time_str_finally = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cs_console.print(f"\n[bold green]脚本整体执行完毕于: {current_time_str_finally}[/bold green]")
    cs_console.print(f"脚本整体运行时间: {overall_duration:.2f} 秒 ({datetime.timedelta(seconds=overall_duration)}).")
    logging.info(f"脚本整体执行完毕于: {current_time_str_finally}. 总耗时: {overall_duration:.2f} 秒.")

    if db_conn:
        db_conn.close()
        logging.info("数据库连接已关闭。")
        cs_console.print(f"[green]INFO:[/green] 数据库连接已关闭。")
