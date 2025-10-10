import argparse
import base64
import datetime
import ipaddress
import json
import logging
import os
import os.path
import re
import shutil
import sqlite3
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
# --- 新增：引入difflib用于相似度比对 ---
from difflib import SequenceMatcher
from urllib.parse import urlparse

import pandas as pd
import requests
import urllib3
from rich.console import Console
# --- 新增：引入rich的Progress模块 ---
from rich.progress import Progress

# --- Global Configuration (全局配置，部分可被命令行参数覆盖) ---
OUTPUT_BASE_DIR = "results_default"
DB_FILE = "icp_asset_cache.db"
CACHE_EXPIRY_HOURS = 30 * 24

# --- Rich Console (用于美化终端输出) ---
cs_console = Console(log_path=False)

# --- Global Constants (全局常量) ---
API_KEY = ""  # !!! 请替换为您的有效 Quake API Key !!!
BASE_URL = "https://quake.360.net/api/v3"
INPUT_FILE = "icpCheck.txt"
BATCH_SIZE = 2000
DELAY = 2  # <--- 默认的API请求2秒
DELAY_BETWEEN_TARGETS = 2  # <--- 新增：定义查询不同目标之间的延时（秒）

# --- Fofa配置 ---
FOFA_KEY = ""
FOFA_BASE_URL = "https://fofa.info"

# --- wer.plus API配置 ---
WERPLUS_API_KEY = ""

QUAKE_PREFLIGHT_LIMIT = 5000  # --- 新增：Quake单个目标资产数量限额，超出直接抛弃 ---
FOFA_IP_PREFLIGHT_LIMIT = 300  # --- 新增：Fofa单个IP资产数量限额，超出直接抛弃 ---

DEFAULT_PORTS = {
    21, 22, 23, 80, 81, 82, 88, 389, 443, 444, 445, 631, 873, 943, 1099, 1433, 1521, 1883, 1936, 2022, 2049, 2082, 2083,
    2086, 2087, 2095, 2096, 2222, 2375, 2376, 2379, 2483, 2484, 3000, 3306, 3307, 3389, 4000, 4001, 4040, 4502, 4503,
    4848, 5000, 5001, 5003, 5005, 5050, 5432, 5433, 5601, 5683, 5800, 5900, 5901, 5984, 5985, 5986, 6080, 6379, 6443,
    7000, 7001, 7002, 7070, 7071, 7080, 7473, 7474, 7777, 7848, 8000, 8001, 8003, 8005, 8006, 8008, 8009, 8010, 8020,
    8069, 8080, 8081, 8082, 8083, 8086, 8087, 8088, 8089, 8090, 8091, 8096, 8161, 8180, 8200, 8400, 8404, 8443, 8500,
    8501, 8529, 8787, 8800, 8848, 8849, 8880, 8883, 8887, 8888, 8980, 8983, 9000, 9001, 9002, 9021, 9042, 9043, 9060,
    9080, 9090, 9091, 9092, 9093, 9200, 9201, 9300, 9418, 9443, 9500, 9600, 9848, 9849, 9876, 9990, 9999, 10000, 10001,
    10022, 10080, 10250, 10256, 10257, 10259, 10443, 11211, 13306, 13389, 15432, 15601, 15672, 16379, 18080, 18200,
    18500, 18848, 19000, 19001, 19090, 19200, 20000, 22002, 25672, 26257, 27017, 28017, 30000, 33060, 33890, 35357, 50000,
    50070, 50075, 50090, 53083
}
# QUAKE_QUERY_TEMPLATE = '(icp_keywords:"{target}" or cert:"{target}") and country:"China" AND not province:"Hongkong" AND NOT domain_is_wildcard:true'
QUAKE_QUERY_TEMPLATE = '(icp_keywords:"{target}" or cert:"{target}") and country:"China" AND not province:"Hongkong"'
SHOW_SCAN_INFO = False

# ======================= 自定义过滤配置 =======================
# 在此列表中定义需要过滤的产品关键字。
# 只要Quake指纹的 product_type 或 product_name_cn 中包含列表内的任何一个词，
# 对应的IP就会被识别为共享服务IP并从主动扫描目标中排除。
PRODUCT_KEYWORDS_TO_FILTER = [
    # # CDN服务（经测试，会遗漏部分真实ip）
    # "内容分发网络(CDN)",
    "知道创宇云安全 CDN",

    # 邮件服务 (已扩展)
    "企业邮箱",
    "邮箱",
    "邮件服务",
    "云邮",
    "腾讯",
    "全球邮"
]


# ======================= 日志与数据库初始化 =======================
def configure_logging(log_file_path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logging.info(f"日志已配置，将记录到: {log_file_path}")


def initialize_database():
    """
    [最终修正完整版] 数据库初始化函数。
    - 确保与原始脚本的表结构完全一致。
    - 对 CompanyAppCache 表进行了修改，增加了 raw_json_web 字段来缓存网站备案信息。
    - 增加了对旧数据库表的兼容性更新逻辑，确保平滑升级。
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # --- 1. 包含原始脚本中的所有表创建语句 ---
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS Targets (target_id INTEGER PRIMARY KEY AUTOINCREMENT, target_name TEXT UNIQUE NOT NULL, last_queried_quake TIMESTAMP, last_queried_fofa TIMESTAMP, notes TEXT);")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS QuakeRawData (data_id INTEGER PRIMARY KEY AUTOINCREMENT, target_id INTEGER NOT NULL, query_timestamp TIMESTAMP NOT NULL, raw_json TEXT NOT NULL, FOREIGN KEY (target_id) REFERENCES Targets (target_id));")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS FofaRuns (fofa_run_id INTEGER PRIMARY KEY AUTOINCREMENT, target_id INTEGER NOT NULL, run_timestamp TIMESTAMP NOT NULL, status TEXT DEFAULT 'pending', input_ip_count INTEGER DEFAULT 0, found_results_count INTEGER DEFAULT 0, notes TEXT, FOREIGN KEY (target_id) REFERENCES Targets (target_id));")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS FofaRawData (fofa_data_id INTEGER PRIMARY KEY AUTOINCREMENT, fofa_run_id INTEGER NOT NULL, raw_json TEXT NOT NULL, FOREIGN KEY (fofa_run_id) REFERENCES FofaRuns (fofa_run_id));")

        # --- 2. 核心修改：创建新的 CompanyAppCache 表结构 ---
        # （注意：这里与原始脚本不同，直接创建了包含新字段的表）
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS CompanyAppCache (
                cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT UNIQUE NOT NULL,
                last_queried TIMESTAMP NOT NULL,
                raw_json_web TEXT,
                raw_json_apps TEXT,
                raw_json_miniprograms TEXT
            );"""
        )
        conn.commit()

        # --- 3. 兼容性更新：为已存在的旧数据库表添加新列 ---
        try:
            # 对应原始脚本中的 "ALTER TABLE Targets"
            cursor.execute("ALTER TABLE Targets ADD COLUMN last_queried_fofa TIMESTAMP;")
        except sqlite3.OperationalError:
            pass  # 字段已存在，忽略错误

        try:
            # 为旧的 CompanyAppCache 表（如果存在且没有web字段）添加新字段
            cursor.execute("ALTER TABLE CompanyAppCache ADD COLUMN raw_json_web TEXT;")
            logging.info("数据库 'CompanyAppCache' 表已成功升级，添加 'raw_json_web' 字段。")
        except sqlite3.OperationalError:
            pass  # 字段已存在，忽略错误

        conn.commit()

        # --- 4. 包含原始脚本中的所有索引创建语句 ---
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_target_name ON Targets (target_name);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_quakerawdata_target_id_timestamp ON QuakeRawData (target_id, query_timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fofaruns_target_id ON FofaRuns (target_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fofarawdata_run_id ON FofaRawData (fofa_run_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_company_name_cache ON CompanyAppCache (company_name);")
        conn.commit()

        logging.info(f"数据库 '{DB_FILE}' 初始化成功。")
        return conn
    except sqlite3.Error as e:
        logging.error(f"数据库初始化失败: {e}", exc_info=True)
        cs_console.print(f"[bold red]Error:[/bold red] 数据库初始化失败: {e}")
        if conn:
            conn.close()
        return None


# ======================= 通用辅助函数 =======================
def load_queries(file_path):
    logging.info(f"开始从文件加载查询目标: {file_path}")
    if not os.path.exists(file_path):
        cs_console.print(f"[bold red]Error:[/bold red] 输入文件不存在: {file_path}")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]
    cs_console.print(f"[green]INFO:[/green] 从 '{file_path}' 加载了 {len(queries)} 个有效的查询目标。")
    return queries


def sanitize_sheet_name(name):
    invalid_chars = r'\[]:\*?/\\'
    name_str = str(name) if name is not None else "Unknown"
    return re.sub(f'[{re.escape(invalid_chars)}]', '', name_str)[:60]


def generate_filename_suffix(company_name, stage=""):
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M")
    sanitized_company_name = sanitize_sheet_name(company_name)
    return f"_{sanitized_company_name}_{stage}_{timestamp_str}"


def get_target_id_from_db(target_name, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT target_id FROM Targets WHERE target_name = ?", (target_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            cursor.execute("INSERT INTO Targets (target_name) VALUES (?)", (target_name,))
            db_conn.commit()
            logging.info(f"数据库中未找到目标'{target_name}'，已创建新条目。")
            return cursor.lastrowid
    except Exception as e:
        logging.error(f"获取/创建 target_id 失败 (目标: {target_name}): {e}", exc_info=True)
        return None


# ======================= 数据查询与解析 (Quake, Fofa, APP) =======================
def check_and_get_quake_cache(target_name, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT target_id, last_queried_quake FROM Targets WHERE target_name = ?", (target_name,))
        target_row = cursor.fetchone()
        if not target_row or not target_row[1]: return None

        target_id, last_queried_str = target_row
        try:
            if '.' in last_queried_str:
                last_queried_dt = datetime.datetime.strptime(last_queried_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_queried_dt = datetime.datetime.strptime(last_queried_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            last_queried_dt = datetime.datetime.fromisoformat(last_queried_str)

        if (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600 >= CACHE_EXPIRY_HOURS:
            return None

        cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ?", (target_id,))
        cached_rows = cursor.fetchall()
        if not cached_rows: return None

        # [核心修改] 直接返回从数据库解析出的原始JSON对象列表
        raw_json_list = [json.loads(row[0]) for row in cached_rows]
        cs_console.print(
            f"    [green]缓存命中:[/green] '{target_name}' 从数据库加载了 {len(raw_json_list)} 条原始Quake记录。")
        return raw_json_list

    except (sqlite3.Error, json.JSONDecodeError, ValueError) as e:
        logging.error(f"检查Quake缓存时出错 ({target_name}): {e}", exc_info=True)
        return None


def query_all_pages(target_name, db_conn):
    """
    [重构版] 查询Quake所有分页数据，并增加了预检机制。
    - 返回值:
      - list: 成功获取的数据列表。
      - 'skipped': 表示因资产过多而被跳过。
      - None: 表示查询失败或API出错。
    """
    cs_console.print(f"    [blue]API查询:[/blue] 目标 '{target_name}'，开始通过Quake API获取数据...")
    headers = {"X-QuakeToken": API_KEY, "Content-Type": "application/json"}
    query_dsl = QUAKE_QUERY_TEMPLATE.format(target=target_name)
    all_raw_data = []

    # --- 新增：预检步骤 ---
    try:
        cs_console.print("      [dim]执行预检: 查询总数据量...[/dim]")
        Init_SIZE = 1
        preflight_params = {"query": query_dsl, "size": Init_SIZE, "ignore_cache": False}
        preflight_response = requests.post(f"{BASE_URL}/search/quake_service", headers=headers, json=preflight_params,
                                           timeout=30)
        preflight_response.raise_for_status()
        preflight_result = preflight_response.json()
        # cs_console.print(f"[dim]预检请求 DSL: {query_dsl}[/dim]")
        # cs_console.print(f"[dim]预检返回结果: {json.dumps(preflight_result, indent=2, ensure_ascii=False)}[/dim]")

        if preflight_result.get("code") != 0:
            cs_console.print(
                f"[bold red]Error (预检):[/bold red] Quake API 查询失败: {preflight_result.get('message')}")
            return None

        meta_data = preflight_result.get("meta", {})
        pagination_data = meta_data.get("pagination", {})

        # 直接获取 total 值（根据文档，total 是整数 34）
        total_assets = pagination_data.get("total", 0)

        cs_console.print(f"      [dim]预检完成，发现总资产数: {total_assets}[/dim]")

        if total_assets > QUAKE_PREFLIGHT_LIMIT:
            cs_console.print(
                f"      [yellow]跳过:[/yellow] 资产数量 ({total_assets}) 超出阈值 ({QUAKE_PREFLIGHT_LIMIT})，已跳过此目标。")
            # 注意：此处不进行任何数据库操作，以便下次运行时可以重新预检
            return 'skipped'  # 返回特殊状态'skipped'

    except requests.exceptions.RequestException as e:
        cs_console.print(f"[bold red]Error (预检):[/bold red] Quake API 请求异常: {e}")
        return None
    # --- 预检结束 ---

    # --- 核心查询逻辑 (仅在预检通过后执行) ---
    pagination_id = None
    try:
        # 如果总数为0，直接返回空列表，无需发起滚动查询
        if total_assets == 0:
            cs_console.print(f"    [green]API查询成功:[/green] 目标资产为0，无需拉取。")
            return []

        while True:
            params = {"query": query_dsl, "size": BATCH_SIZE, "ignore_cache": False, "latest": True}
            if pagination_id:
                params["pagination_id"] = pagination_id

            # cs_console.print(f"    [blue]API查询:[/blue] 目标 '{target_name}'，获取全部数据...")
            response = requests.post(f"{BASE_URL}/scroll/quake_service", headers=headers, json=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != 0:
                cs_console.print(f"[bold red]Error:[/bold red] Quake API 查询失败: {result.get('message')}")
                # 如果中途失败，返回已获取的部分数据，或根据策略返回None
                return None

            current_batch = result.get("data", [])
            if not current_batch:
                break

            all_raw_data.extend(current_batch)
            pagination_id = result.get("meta", {}).get("pagination_id")
            if not pagination_id:
                break

            time.sleep(DELAY)

        cs_console.print(f"    [green]API查询成功:[/green] 共获取 {len(all_raw_data)} 条原始记录。")

        # --- 数据库缓存逻辑 (成功获取数据后执行) ---
        cursor = db_conn.cursor()
        timestamp = datetime.datetime.now()
        target_id = get_target_id_from_db(target_name, db_conn)
        if not target_id: return None  # 如果获取target_id失败则不继续

        cursor.execute("DELETE FROM QuakeRawData WHERE target_id = ?", (target_id,))
        if all_raw_data:
            data_to_insert = [(target_id, timestamp, json.dumps(item, ensure_ascii=False)) for item in all_raw_data]
            cursor.executemany("INSERT INTO QuakeRawData (target_id, query_timestamp, raw_json) VALUES (?, ?, ?)",
                               data_to_insert)

        cursor.execute("UPDATE Targets SET last_queried_quake = ? WHERE target_id = ?", (timestamp, target_id))
        db_conn.commit()

        return all_raw_data

    except requests.exceptions.RequestException as e:
        cs_console.print(f"[bold red]Error:[/bold red] Quake API 请求异常: {e}")
        return None
    except Exception as e:
        logging.error(f"Quake API处理中发生未知异常 ({target_name}): {e}", exc_info=True)
        return None


def parse_results(raw_data_list_objs, target_name_for_fallback=""):
    """
    [重构版] 解析Quake数据，实现多源归属，并为最终的汇总报告生成Favicon查询数据。

    - 核心功能:
      1. 按优先级 (ICP > 证书 > ASN) 确定资产的“主体单位”。
      2. (已修正) 确保能正确提取证书subject中的organization字段。
      3. 收集所有单位的Favicon查询语句，用于生成最终的全局汇总报告。
      4. 不再向单位独立报告中添加Favicon查询行。

    - 返回值:
      - parsed_results (list): 包含所有已解析资产的干净列表。
      - favicon_summary_data (list): 用于生成全局汇总报告的简化数据列表。
    """
    # 步骤 1: 遍历原始API数据，逐条进行解析和标准化
    parsed_results = []
    for raw_data_obj in raw_data_list_objs:
        service_info = raw_data_obj.get("service", {})
        http_info = service_info.get("http", {})
        tls_info = service_info.get("tls", {})
        icp_info = http_info.get("icp", {})
        main_icp = icp_info.get("main_licence", {}) if icp_info else {}
        # 沿着正确的嵌套结构层层深入
        cert_parsed = tls_info.get("handshake_log", {}).get("server_certificates", {}).get("certificate", {}).get(
            "parsed", {})
        cert_subject = cert_parsed.get("subject", {})

        company_name, attribution_source = "", ""
        # 优先级 1: 检查是否存在最准确的ICP备案信息
        if main_icp.get("unit"):
            company_name, attribution_source = main_icp["unit"], "ICP备案"

        # --- 核心修正 ---
        # 优先级 2: 确保从正确的路径 (subject -> organization) 提取证书单位
        elif cert_subject.get("organization"):
            company_name, attribution_source = cert_subject["organization"][0], "证书信息"

        # 优先级 3: 如果以上都没有，则使用ASN组织信息作为最后的保底归属
        elif raw_data_obj.get("org"):
            company_name, attribution_source = raw_data_obj["org"], "ASN信息"

        location_info = raw_data_obj.get("location", {})
        port = raw_data_obj.get("port")
        url_for_display, scan_urls = "", set()
        direct_url = (http_info.get("http_load_url") or [http_info.get("url")])[0]
        if direct_url:
            url_for_display, scan_urls = direct_url, {direct_url}
        else:
            host = http_info.get("host") or raw_data_obj.get("domain") or raw_data_obj.get("ip")
            if host:
                http_url = f"http://{host}" + (f":{port}" if port not in [80, None] else "")
                https_url = f"https://{host}" + (f":{port}" if port not in [443, None] else "")
                scan_urls.update([http_url, https_url])
                url_for_display = http_url
        components = raw_data_obj.get("components", [])
        fingerprints = [f"{c.get('product_name_cn') or c.get('product_name_en')}" + (
            f" ({c.get('version', '')})" if c.get('version') else "") for c in components if
                        c.get('product_name_cn') or c.get('product_name_en')]
        fingerprints_str = "\n".join(fingerprints)
        clean_title = (http_info.get("title", "") or "").replace("\n", " ").replace("\r", " ").strip()
        favicon_hash = http_info.get("favicon", {}).get("hash", "")

        parsed = {
            "IP": raw_data_obj.get("ip", ""), "Port": str(port or ""), "Host": http_info.get("host", ""),
            "URL": url_for_display, "HTTP状态码": http_info.get("status_code", ""), "scan_urls": list(scan_urls),
            "Domain": raw_data_obj.get("domain", ""), "网站标题": clean_title, "产品指纹": fingerprints_str,
            "备案号": icp_info.get("licence", "") if icp_info else "", "主体单位": company_name,
            "归属判定来源": attribution_source, "备案单位类型": main_icp.get("nature", ""),
            "时间": raw_data_obj.get("time", ""),
            "归属省份": location_info.get("province_cn", ""), "components": components, "favicon_hash": favicon_hash
        }
        parsed_results.append(parsed)

    # 步骤 2: 按上一步解析出的“主体单位”对所有资产进行分组
    assets_by_company = defaultdict(list)
    for item in parsed_results:
        company_key = item.get("主体单位") or f"未知主体单位_{target_name_for_fallback}"
        assets_by_company[company_key].append(item)

    # 步骤 3: 遍历每个单位的分组，仅为全局汇总报告生成Favicon查询语句
    favicon_summary_data = []
    for company_name, assets in assets_by_company.items():
        favicons = {item.get("favicon_hash") for item in assets if item.get("favicon_hash")}

        if favicons:
            favicon_query_string = " OR ".join([f'favicon: "{h}"' for h in favicons])
            favicon_summary_data.append({"目标单位": company_name, "favicon关联语句": favicon_query_string})

    return parsed_results, favicon_summary_data


def run_cdn_analyzer(ip_list, base_output_dir):
    """
    调用外部工具 cdnAnalyzer.exe 对IP列表进行CDN识别。

    :param ip_list: 待检测的IP列表 (list of strings)。
    :param base_output_dir: 一个基础路径，用于存放临时文件。
    :return: 一个包含被识别为CDN的IP的集合 (set of strings)。
    """
    cs_console.print("    [blue]执行:[/blue] 调用 cdnAnalyzer 进行深度CDN分析...")

    # 1. 定义工具和文件的路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cdn_analyzer_dir = os.path.join(script_dir, 'tools', 'cdnAnalyzer')
    cdn_analyzer_exe = os.path.join(cdn_analyzer_dir, 'cdnAnalyzer.exe')
    config_file = os.path.join(cdn_analyzer_dir, 'config.yaml')

    # 检查工具是否存在
    if not os.path.exists(cdn_analyzer_exe):
        cs_console.print(f"      [yellow]Warning:[/yellow] cdnAnalyzer.exe 未找到，跳过此分析步骤。")
        return set()

    # 创建临时文件存放目录
    temp_dir = os.path.join(base_output_dir, "temp_cdn_check")
    os.makedirs(temp_dir, exist_ok=True)

    # 2. 准备临时输入和输出文件
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    input_file = os.path.join(temp_dir, f"ips_to_check_{timestamp}.txt")
    output_file = os.path.join(temp_dir, f"cdn_results_{timestamp}.json")

    try:
        # 写入IP到输入文件
        with open(input_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(ip_list))

        # 3. 构建并执行命令
        command = [
            cdn_analyzer_exe,
            '-c', config_file,
            '-I', 'file',
            '-i', input_file,
            '-O', 'json',
            '-o', output_file
        ]

        subprocess.run(command, check=True, cwd=cdn_analyzer_dir, capture_output=True, text=True)

        # 4. 读取并解析JSON输出结果
        cdn_ips = set()
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                try:
                    results = json.load(f)
                    for item in results:
                        # 根据test.json文件，检查is_cdn字段
                        if item.get("is_cdn") is True:
                            cdn_ips.add(item.get("raw"))
                except json.JSONDecodeError:
                    cs_console.print(f"      [bold red]Error:[/bold red] 解析 cdnAnalyzer 输出文件失败。")

        cs_console.print(f"      [green]Success:[/green] cdnAnalyzer分析完成，识别出 {len(cdn_ips)} 个CDN IP。")
        return cdn_ips

    except FileNotFoundError:
        cs_console.print(f"      [bold red]Error:[/bold red] cdnAnalyzer.exe 未找到于 '{cdn_analyzer_exe}'")
        return set()
    except subprocess.CalledProcessError as e:
        cs_console.print(f"      [bold red]Error:[/bold red] cdnAnalyzer 执行失败。错误信息: {e.stderr}")
        return set()
    except Exception as e:
        cs_console.print(f"      [bold red]Error:[/bold red] cdnAnalyzer 处理过程中发生未知错误: {e}")
        return set()
    finally:
        # 5. 清理临时文件夹 <--- 核心修改处
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def identify_shared_service_ips(raw_quake_data_list):
    cs_console.print("    [blue]执行:[/blue] 智能过滤IP，以优化Fofa反查目标...")
    all_ips = {item.get("ip") for item in raw_quake_data_list if item.get("ip")}

    # 第一层过滤：首先调用 cdnAnalyzer 进行专业分析
    cdn_ips_from_analyzer = run_cdn_analyzer(list(all_ips), OUTPUT_BASE_DIR)
    shared_service_ips = cdn_ips_from_analyzer

    # 第二层过滤：执行启发式规则进行补充分析
    SAN_THRESHOLD = 20
    PUBLIC_SERVICE_KEYWORDS = [
        'qiye.aliyun.com', 'exmail.qq.com', 'qiye.163.com', 'ali-mail.com', 'dingtalk.com', 'mxhichina.com',
        '.cdn.cloudflare.net', '.akamaiedge.net', '.fastly.net', '.chinacache.com', '.cdnetworks.net', 'aliyuncs.com',
        'bspapp.com', 'hiflow.tencent.com'
    ]

    for raw_data in raw_quake_data_list:
        ip = raw_data.get("ip")
        if not ip or ip in shared_service_ips:
            continue

        # 规则A: TLS证书SAN数量判断 (已恢复)
        # try:
        #     if len(raw_data['service']['tls']['certificate']['parsed']['extensions']['subject_alt_name'][
        #                'dns_names']) > SAN_THRESHOLD:
        #         shared_service_ips.add(ip)
        #         continue
        # except (KeyError, TypeError):
        #     pass

        # 规则B: 产品指纹关键字判断 (已修复)
        try:
            for component in raw_data.get('components', []):
                product_name_cn = component.get('product_name_cn', '')
                product_types_list = component.get('product_type', [])

                if any(keyword in product_name_cn for keyword in PRODUCT_KEYWORDS_TO_FILTER):
                    shared_service_ips.add(ip)
                    break

                if isinstance(product_types_list, list):
                    for p_type in product_types_list:
                        if any(keyword in p_type for keyword in PRODUCT_KEYWORDS_TO_FILTER):
                            shared_service_ips.add(ip)
                            break
                    else:
                        continue
                    break
        except (KeyError, TypeError):
            pass

        if ip in shared_service_ips:
            continue

        # 规则C: CNAME记录判断 (已恢复)
        # try:
        #     for record in raw_data.get('service', {}).get('dns', {}).get('cname', []):
        #         if any(keyword in record.lower() for keyword in PUBLIC_SERVICE_KEYWORDS):
        #             shared_service_ips.add(ip)
        #             break
        # except (KeyError, TypeError):
        #     pass
        #
        # if ip in shared_service_ips:
        #     continue

        # 规则D: HTTP响应头判断 (已恢复)
        try:
            if 'Aliyun URL Forwarding Server' in raw_data.get('service', {}).get('http', {}).get('response_headers',
                                                                                                 ''):
                shared_service_ips.add(ip)
        except (KeyError, TypeError):
            pass

    # 最终计算结果
    clean_ips = all_ips - shared_service_ips
    cs_console.print(f"      [green]Success:[/green] IP过滤完成。")
    cs_console.print(f"        - [dim]原始IP总数: {len(all_ips)}[/dim]")
    cs_console.print(f"        - [dim]识别出共享/CDN IP数: {len(shared_service_ips)}[/dim]")
    cs_console.print(f"        - [dim]剩余独立IP数: {len(clean_ips)}[/dim]")
    return list(clean_ips), list(shared_service_ips)


def check_and_get_fofa_cache(target_id, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT last_queried_fofa FROM Targets WHERE target_id = ?", (target_id,))
        target_row = cursor.fetchone()
        if not target_row or not target_row[0]: return None
        last_queried_fofa_str = target_row[0]
        try:
            if '.' in last_queried_fofa_str:
                last_queried_dt = datetime.datetime.strptime(last_queried_fofa_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_queried_dt = datetime.datetime.strptime(last_queried_fofa_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            last_queried_dt = datetime.datetime.fromisoformat(last_queried_fofa_str)
        if (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600 >= CACHE_EXPIRY_HOURS: return None
        cursor.execute(
            "SELECT fr.fofa_run_id FROM FofaRuns fr WHERE fr.target_id = ? AND fr.status = 'completed' ORDER BY fr.run_timestamp DESC LIMIT 1",
            (target_id,))
        fofa_run_row = cursor.fetchone()
        if not fofa_run_row: return None
        fofa_run_id = fofa_run_row[0]
        cursor.execute("SELECT raw_json FROM FofaRawData WHERE fofa_run_id = ?", (fofa_run_id,))
        cached_rows = cursor.fetchall()
        if not cached_rows: return None
        raw_results_from_cache = [item for row in cached_rows for item in json.loads(row[0])]
        parsed_data = parse_fofa_results(raw_results_from_cache)
        cs_console.print(f"    [green]Fofa缓存命中:[/green] 从数据库加载并解析 {len(parsed_data)} 条Fofa记录。")
        return parsed_data
    except (sqlite3.Error, json.JSONDecodeError, ValueError) as e:
        logging.error(f"检查Fofa缓存时出错 (ID: {target_id}): {e}", exc_info=True)
        return None


def query_fofa_by_ips(ip_list, target_id, db_conn):
    """
    [重构版] 通过Fofa API查询IP相关资产，增加了对每个IP的预检机制。
    """
    if not ip_list: return [], None

    # --- 新增：逐个IP进行预检 ---
    cs_console.print(f"    [blue]执行:[/blue] 开始对 {len(ip_list)} 个IP进行Fofa预检...")
    valid_ips_to_query = []
    skipped_ips_info = []

    # 使用 rich.progress 创建一个进度条来优化用户体验
    with Progress(console=cs_console) as progress:
        task = progress.add_task("[cyan]IP预检中...", total=len(ip_list))
        for ip in ip_list:
            try:
                query_str = f'ip="{ip}"'
                qbase64 = base64.b64encode(query_str.encode('utf-8')).decode('utf-8')
                preflight_url = f"{FOFA_BASE_URL}/api/v1/search/all?key={FOFA_KEY}&qbase64={qbase64}&size=1&fields=host"

                preflight_response = requests.get(preflight_url, timeout=10)
                preflight_response.raise_for_status()
                preflight_result = preflight_response.json()

                if preflight_result.get("error"):
                    skipped_ips_info.append({'ip': ip, 'reason': f"API错误: {preflight_result.get('errmsg')}"})
                else:
                    total_results = preflight_result.get("size", 0)
                    if total_results > FOFA_IP_PREFLIGHT_LIMIT:
                        skipped_ips_info.append(
                            {'ip': ip, 'reason': f'结果数({total_results}) > 阈值({FOFA_IP_PREFLIGHT_LIMIT})'})
                    else:
                        valid_ips_to_query.append(ip)
            except requests.exceptions.RequestException as e:
                skipped_ips_info.append({'ip': ip, 'reason': f'网络请求异常: {e}'})

            progress.update(task, advance=1)

    cs_console.print(
        f"      [green]Success:[/green] 预检完成。有效IP: {len(valid_ips_to_query)} 个, 跳过IP: {len(skipped_ips_info)} 个。")
    if skipped_ips_info:
        logging.info(f"Fofa预检跳过的IP: {skipped_ips_info}")

    if not valid_ips_to_query:
        cs_console.print("      [yellow]INFO:[/yellow] 预检后无有效IP可供查询。")
        return [], None
    # --- 预检结束 ---

    # --- 核心查询逻辑 (仅对通过预检的IP执行) ---
    fofa_run_id = None
    try:
        cursor = db_conn.cursor()
        cursor.execute("INSERT INTO FofaRuns (target_id, run_timestamp, status, input_ip_count) VALUES (?, ?, ?, ?)",
                       (target_id, datetime.datetime.now(), 'running', len(valid_ips_to_query)))
        db_conn.commit()
        fofa_run_id = cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Fofa: 创建FofaRuns记录失败: {e}", exc_info=True)
        return [], None

    all_fofa_raw_results = []
    api_call_overall_successful = True

    # 使用通过预检的IP列表进行批量查询
    ip_chunks = [valid_ips_to_query[i:i + 100] for i in range(0, len(valid_ips_to_query), 100)]
    cs_console.print(f"    [blue]执行:[/blue] 开始从Fofa API获取数据 (共 {len(ip_chunks)} 个查询批次)...")

    for index, chunk in enumerate(ip_chunks, 1):
        if not api_call_overall_successful: break
        cs_console.print(f"      [dim]正在查询批次 ({index}/{len(ip_chunks)})...[/dim]")
        query_str = " || ".join([f'ip="{ip}"' for ip in chunk])
        qbase64 = base64.b64encode(query_str.encode('utf-8')).decode('utf-8')
        next_id = None
        while True:
            try:
                fields = "host,ip,port,protocol,title,server,icp,domain,link"
                api_url = f"{FOFA_BASE_URL}/api/v1/search/next?key={FOFA_KEY}&qbase64={qbase64}&fields={fields}&size=2000"
                if next_id: api_url += f"&next={next_id}"
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                result = response.json()
                if result.get("error"):
                    cs_console.print(f"    [bold red]Error (批次 {index}):[/bold red] Fofa API: {result.get('errmsg')}")
                    api_call_overall_successful = False;
                    break
                batch_results = result.get("results", [])
                if batch_results: all_fofa_raw_results.extend(batch_results)
                next_id = result.get("next")
                if not next_id: break
                time.sleep(DELAY)
            except requests.exceptions.RequestException as e:
                cs_console.print(f"    [bold red]Error (批次 {index}):[/bold red] Fofa API请求异常: {e}")
                api_call_overall_successful = False;
                break

    if api_call_overall_successful:
        cs_console.print(f"      [green]Success:[/green] Fofa查询完成，共获取 {len(all_fofa_raw_results)} 条记录。")

    final_status = 'failed'
    if api_call_overall_successful:
        if all_fofa_raw_results:
            try:
                cursor = db_conn.cursor()
                data_chunks = [all_fofa_raw_results[i:i + 100] for i in range(0, len(all_fofa_raw_results), 100)]
                data_to_insert = [(fofa_run_id, json.dumps(chunk, ensure_ascii=False)) for chunk in data_chunks]
                cursor.executemany("INSERT INTO FofaRawData (fofa_run_id, raw_json) VALUES (?, ?)", data_to_insert)
                final_status = 'completed'
            except sqlite3.Error:
                final_status = 'completed_with_errors'
        else:
            final_status = 'completed'
    try:
        cursor = db_conn.cursor()
        cursor.execute("UPDATE FofaRuns SET status = ?, found_results_count = ? WHERE fofa_run_id = ?",
                       (final_status, len(all_fofa_raw_results), fofa_run_id))
        if final_status.startswith('completed'):
            cursor.execute("UPDATE Targets SET last_queried_fofa = ? WHERE target_id = ?",
                           (datetime.datetime.now(), target_id))
        db_conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Fofa: 更新数据库失败: {e}", exc_info=True)
        db_conn.rollback()

    return all_fofa_raw_results, fofa_run_id


def parse_fofa_results(fofa_raw_data_list):
    parsed_results = []
    fields_order = ["host", "ip", "port", "protocol", "title", "server", "icp", "domain", "link"]
    for item_list in fofa_raw_data_list:
        if not isinstance(item_list, list) or len(item_list) != len(fields_order): continue
        fofa_item = dict(zip(fields_order, item_list))
        url = fofa_item.get("link", "")
        if not url:
            protocol, host, port = fofa_item.get("protocol", "").lower(), fofa_item.get("host", ""), fofa_item.get(
                "port", 80)
            if "://" in host:
                url = host
            elif protocol and host:
                if ":" in host:
                    url = f"{protocol}://{host}"
                else:
                    url = f"{protocol}://{host}" + (f":{port}" if (protocol == 'http' and port != 80) or (
                            protocol == 'https' and port != 443) else "")
        parsed = {
            "IP": fofa_item.get("ip", ""), "Port": str(fofa_item.get("port", "")), "Host": fofa_item.get("host", ""),
            "URL": url, "Domain": fofa_item.get("domain", ""), "网站标题": fofa_item.get("title", ""),
            "备案号": fofa_item.get("icp", ""), "Server": fofa_item.get("server", "")
        }
        parsed_results.append(parsed)
    return parsed_results


def check_and_get_icp_asset_cache(company_name, db_conn, types_to_check):
    """
    [重构版] 检查并获取ICP资产缓存，支持增量检查。
    - 返回值1: 一个字典，包含已缓存且有效的数据。
    - 返回值2: 一个列表，包含本次需要从API实时查询的资产类型。
    """
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT last_queried, raw_json_web, raw_json_apps, raw_json_miniprograms FROM CompanyAppCache WHERE company_name = ?",
            (company_name,))
        row = cursor.fetchone()

        # 如果根本没有缓存记录，则所有类型都需要从API获取
        if not row:
            return {}, list(types_to_check)

        last_queried_dt = datetime.datetime.fromisoformat(row[0])
        # 如果缓存已过期，则所有类型都需要重新从API获取
        if (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600 >= CACHE_EXPIRY_HOURS:
            return {}, list(types_to_check)

        # 缓存未过期，进行精细化检查
        cached_data = {}
        missing_types = []

        # 字段名与类型的映射
        type_to_column = {
            'web': row[1],
            'app': row[2],
            'mapp': row[3]
        }

        for asset_type in types_to_check:
            # 检查对应列的数据是否存在 (不是NULL)
            if asset_type in type_to_column and type_to_column[asset_type] is not None:
                # 存在，则从缓存加载
                cached_data[asset_type] = json.loads(type_to_column[asset_type])
            else:
                # 不存在，则加入待查询列表
                missing_types.append(asset_type)

        return cached_data, missing_types

    except Exception as e:
        logging.error(f"检查ICP资产缓存时出错: {e}", exc_info=True)
        # 出错时，为保证数据准确性，选择重新查询所有类型
        return {}, list(types_to_check)


def query_icp_assets_from_wer(company_name, db_conn, types_to_check):
    """
    [重构优化版] 从wer.plus查询备案资产，实现串行查询和更稳定的API请求。
    - 核心优化: 移除了ThreadPoolExecutor，改为for循环串行调用_fetch_icpb_data，
                 从根本上避免了对单个目标的并发请求。
    """
    # 1. 精细化检查缓存 (逻辑不变)
    cached_data, types_to_fetch_from_api = check_and_get_icp_asset_cache(company_name, db_conn, types_to_check)

    # 2. 如果有需要从API获取的类型，则执行串行查询
    newly_fetched_data = {}
    if types_to_fetch_from_api:
        cs_console.print(
            f"      [blue]API查询:[/blue] 缓存不完整或已过期, 将依次串行查询以下新类型: [cyan]{', '.join(types_to_fetch_from_api)}[/cyan]")

        # <--- 核心优化：使用简单的 for 循环代替线程池，实现串行 --->
        for asset_type in types_to_fetch_from_api:
            try:
                # 每次调用_fetch_icpb_data都会有内置的延时，这里不需要额外sleep
                result_data = _fetch_icpb_data(company_name, asset_type)
                newly_fetched_data[asset_type] = result_data
            except Exception as e:
                logging.error(f"从API获取 '{asset_type}' 类型数据失败: {e}", exc_info=True)
                newly_fetched_data[asset_type] = []  # 出错时返回空列表

    # 3. 如果获取到了新数据，则更新数据库 (逻辑不变)
    if newly_fetched_data:
        try:
            cursor = db_conn.cursor()
            cursor.execute("SELECT cache_id FROM CompanyAppCache WHERE company_name = ?", (company_name,))
            existing_record = cursor.fetchone()

            # 准备要更新或插入的数据
            data_to_save = {
                'web': json.dumps(newly_fetched_data.get('web')),
                'app': json.dumps(newly_fetched_data.get('app')),
                'mapp': json.dumps(newly_fetched_data.get('mapp'))
            }

            if existing_record:
                set_clauses = [f"raw_json_{asset_type.replace('mapp', 'miniprograms')}s = ?" for asset_type in
                               newly_fetched_data.keys() if newly_fetched_data.get(asset_type) is not None]
                params = [json.dumps(data) for asset_type, data in newly_fetched_data.items() if data is not None]
                set_clauses.append("last_queried = ?")
                params.append(datetime.datetime.now())
                params.append(company_name)
                update_sql = f"UPDATE CompanyAppCache SET {', '.join(set_clauses)} WHERE company_name = ?"
                cursor.execute(update_sql, tuple(params))
            else:
                cursor.execute(
                    """INSERT INTO CompanyAppCache 
                       (company_name, last_queried, raw_json_web, raw_json_apps, raw_json_miniprograms) 
                       VALUES (?, ?, ?, ?, ?)""",
                    (company_name, datetime.datetime.now(),
                     data_to_save.get('web'),
                     data_to_save.get('app'),
                     data_to_save.get('mapp'))
                )
            db_conn.commit()
            cs_console.print(f"      [green]缓存操作:[/green] 数据库已为 '{company_name}' 更新/创建缓存。")
        except sqlite3.Error as e:
            logging.error(f"更新/插入ICP资产缓存失败: {e}", exc_info=True)

    # 4. 合并缓存数据和新获取的数据，并返回 (逻辑不变)
    final_results = cached_data.copy()
    final_results.update(newly_fetched_data)

    return final_results


def parse_all_icp_results(raw_data_dict):
    """
    [修正版] 统一的备案资产解析函数。
    - 修正了对输入数据结构的处理，现在能正确处理字典格式的输入。
    """
    parsed_list = []
    # 核心修正：遍历字典的键值对 (例如 'web': [资产列表])
    # raw_data_dict.items() 会同时给出类型(asset_type)和对应的列表(result_list)
    for asset_type, result_list in raw_data_dict.items():
        # 遍历每个资产列表中的具体资产项
        for item in result_list:
            # 增加一个健壮性检查，确保item是字典类型，以防万一
            if not isinstance(item, dict):
                continue

            asset_name = ''
            # 根据资产类型，从不同的字段提取名称
            if asset_type == 'web':
                asset_name = item.get('domain', '')
            elif asset_type in ['app', 'mapp']:
                asset_name = item.get('serviceName', '')

            # 组装成标准格式的字典
            parsed_item = {
                '备案主体': item.get('unitName', ''),
                '类型': asset_type.upper(),
                '名称/域名': asset_name,
                '备案号': item.get('serviceLicence', ''),
                '单位性质': item.get('natureName', ''),
                '审核时间': item.get('updateRecordTime', '')
            }
            parsed_list.append(parsed_item)

    return parsed_list


# ==============================================================================
# ================== 新增 --onlyicp 模式的主流程函数 =====================
# ==============================================================================

def write_onlyicp_report_to_excel(output_dir, all_data):
    """
    [新增] 为 --onlyicp 模式生成最终的Excel汇总报告。
    """
    if not all_data: return
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    excel_path = os.path.join(output_dir, f"ICP_Only_Results_{timestamp}.xlsx")
    try:
        df = pd.DataFrame(all_data)
        df.to_excel(excel_path, index=False, sheet_name="ICP_Assets_Summary")
        cs_console.print(
            f"    [green]Success:[/green] ICP备案资产汇总报告已保存: '{os.path.basename(excel_path)}' (共 {len(all_data)} 条)")
    except Exception as e:
        logging.error(f"保存ICP备案资产汇总报告失败: {e}")
        cs_console.print(f"    [bold red]Error:[/bold red] 保存ICP备案资产汇总报告失败。")


def run_only_icp_mode(db_conn, args):
    """
    [重构优化版] --onlyicp 模式的主函数。
    - 核心优化 1: 在处理不同目标之间增加了 DELAY_BETWEEN_TARGETS 延时。
    - 核心优化 2: 引入 rich.progress 进度条，优化了用户等待体验。
    - 修复了原脚本的数据结构不匹配问题。
    """
    start_time = time.time()
    cs_console.print(f"[bold blue]--onlyicp 模式启动... (已优化API请求频率)[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    grand_total_parsed_assets = []
    types_to_check = ['web', 'app', 'mapp']

    # <--- 核心优化：使用 rich.progress 包装循环 --->
    with Progress(console=cs_console) as progress:
        task = progress.add_task("[cyan]正在查询备案资产...", total=len(target_names))

        for index, target_name in enumerate(target_names):
            progress.update(task, description=f"[cyan]处理中: [bold]{target_name}[/bold]")

            # (这里不再打印开始处理的日志，交由进度条显示)
            # cs_console.print(f"\n[bold magenta]>>>>>> 开始处理目标 ({index + 1}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

            # 1. 查询所有类型的备案资产 (现在内部是串行查询)
            raw_icp_data = query_icp_assets_from_wer(target_name, db_conn, types_to_check)

            if raw_icp_data:
                # 2. 解析数据
                parsed_data = parse_all_icp_results(raw_icp_data)
                cs_console.print(f"  [green]查询并解析成功:[/green] 发现 {len(parsed_data)} 条备案记录。")

                # 3. 为解析后的每一条记录添加“查询目标”字段
                for item in parsed_data:
                    item['查询目标'] = target_name

                # 4. 合并到总列表
                grand_total_parsed_assets.extend(parsed_data)
            else:
                cs_console.print(f"  [yellow]INFO:[/yellow] 未找到 '{target_name}' 相关的备案资产信息。")

            progress.update(task, advance=1)

            # <--- 核心优化：在处理下一个目标前进行延时 --->
            if index < len(target_names) - 1:
                progress.update(task, description=f"[cyan]等待 {DELAY_BETWEEN_TARGETS}秒后继续...")
                time.sleep(DELAY_BETWEEN_TARGETS)

    # 5. 在所有目标处理完毕后，统一写入Excel
    if grand_total_parsed_assets:
        cs_console.print(f"\n[bold blue]开始生成备案资产总报告...[/bold blue]")
        write_onlyicp_report_to_excel(OUTPUT_BASE_DIR, grand_total_parsed_assets)
    else:
        cs_console.print("\n[yellow]INFO:[/yellow] 未发现任何备案资产，不生成总报告。")

    cs_console.print(f"\n[bold green]--onlyicp 模式结束.[/bold green] 总耗时: {round(time.time() - start_time, 2)} 秒.")


def _fetch_icpb_data(company_name, app_type):
    """
    [重构版] 通过wer.plus接口获取备案信息，集成了重试、详细日志和永不超时逻辑。
    - 响应失败后会重试1次。
    - 不设置超时时间，将一直等待服务器响应。
    - 适配了原脚本的日志和输出系统 (logging 和 rich.console)。
    """
    api_url = "https://api2.wer.plus/api/icpb"
    all_results, page = [], 1
    MAX_RETRIES = 2  # 1次原始请求 + 1次重试

    cs_console.print(f"    [blue]执行:[/blue] 开始通过 wer.plus 查询 '{company_name}' 的 '{app_type}' 信息...")
    logging.info(f"wer.plus: 开始查询 '{company_name}' 的 '{app_type}' 信息。")

    while True:
        params = {'key': WERPLUS_API_KEY, 't': company_name, 'page': page, 'pagesize': 40, 'apptype': app_type}

        response = None
        result_json = None

        # --- 重试逻辑循环 ---
        for attempt in range(MAX_RETRIES):
            try:
                log_message_detail = f"请求第 {page} 页 (第 {attempt + 1}/{MAX_RETRIES} 次尝试)"
                logging.info(f"wer.plus: {log_message_detail}")
                if attempt > 0:
                    cs_console.print(f"      [yellow]重试:[/yellow] 正在进行第 {attempt + 1}/{MAX_RETRIES} 次尝试...")
                    time.sleep(1)  # 等待1秒再重试

                # --- 核心修改: timeout=None 表示永不超时 ---
                # 警告: 如果对方服务器无响应，程序会一直卡在这里。
                response = requests.get(api_url, params=params, timeout=None)
                response.raise_for_status()  # 检查HTTP错误 (如 404, 500)
                result_json = response.json()

                # 如果请求和解析都成功，则跳出重试循环
                break

            except requests.exceptions.RequestException as e:
                logging.warning(f"wer.plus: 第 {page} 页网络请求失败 (尝试 {attempt + 1}/{MAX_RETRIES}) - {e}")

            except json.JSONDecodeError:
                logging.warning(f"wer.plus: 第 {page} 页JSON解析失败 (尝试 {attempt + 1}/{MAX_RETRIES})")

        # 如果 for 循环正常结束（即重试次数用尽后仍然失败），result_json 将是 None
        if result_json is None:
            error_msg = f"第 {page} 页在重试 {MAX_RETRIES - 1} 次后仍然失败，中止对该公司该类型的查询。"
            logging.error(f"wer.plus: {error_msg}")
            cs_console.print(f"      [bold red]Error:[/bold red] {error_msg}")
            # 返回已经获取到的数据，而不是空
            return all_results

        # --- API 业务逻辑判断 ---
        if result_json.get("code") != 200:
            msg = result_json.get('msg', '未知API业务错误')
            logging.error(f"wer.plus: API返回错误 - {msg}")
            cs_console.print(f"      [bold red]Error:[/bold red] API返回错误: {msg}")
            break

        data = result_json.get("data", {})
        current_page_results = data.get("list", [])

        if not current_page_results:
            cs_console.print(f"      [dim]当前页未返回任何数据，查询结束。[/dim]")
            break

        all_results.extend(current_page_results)

        # --- 翻页逻辑判断 ---
        current_page_num = data.get("pageNum")
        total_pages = data.get("pages")
        if current_page_num is None or total_pages is None or current_page_num >= total_pages:
            break

        page += 1
        time.sleep(DELAY)  # 使用全局的延迟变量

    if all_results:
        summary_message = f"查询完成! 共找到 {len(all_results)} 条 '{app_type}' 相关记录。"
        logging.info(f"icpApi: {summary_message}")
        cs_console.print(f"      [green]Success:[/green] {summary_message}")
        time.sleep(1)
    else:
        logging.info(f"wer.plus: 查询完成，未找到任何 '{app_type}' 相关记录。")
        cs_console.print(f"      [yellow]INFO:[/yellow] 未找到任何 '{app_type}' 相关记录。")
        # 2025-1002 这里新增的延迟，不然多次查询为空的，没有记录的时候，就没有延迟很快就报错：请求频率超过限制
        time.sleep(DELAY)  # 使用全局的延迟变量

    return all_results


# ======================= 文件输出与处理 =======================
def write_quake_results_to_excel(output_dir, company_name, data, stage=""):
    """(最终格式化版) 保存Quake结果，移除指定列，并防止URL自动超链接。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    excel_path = os.path.join(output_dir, f"quake_result{filename_suffix}.xlsx")

    if not data:
        return

    # --- 核心修改 1: 在创建DataFrame之前，先从每条数据中移除不需要的列 ---
    # <--- 核心修改处：将 'components' 添加到移除列表中
    columns_to_remove = ['components', 'scan_urls', 'Host']

    cleaned_data = []
    for row in data:
        # 创建一个新字典，只包含我们需要的键
        cleaned_row = {k: v for k, v in row.items() if k not in columns_to_remove}
        cleaned_data.append(cleaned_row)

    if not cleaned_data:
        return

    df = pd.DataFrame(cleaned_data)

    try:
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Quake_Data", index=False)

            workbook = writer.book
            worksheet = writer.sheets["Quake_Data"]

            # 创建格式对象
            wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
            top_align_format = workbook.add_format({'valign': 'top'})
            # --- 核心修改 2: 创建一个纯文本格式，防止Excel自动生成超链接 ---
            text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})

            wrap_columns = ['产品指纹', '网站标题']

            for col_num, col_name in enumerate(df.columns):
                # 计算合适的列宽
                try:
                    max_len = max(df[col_name].astype(str).map(len).max(), len(col_name))
                except (ValueError, TypeError):
                    max_len = len(col_name)
                width = min(max_len + 5, 60)

                # --- 核心修改 3: 对URL列应用纯文本格式 ---
                if col_name == 'URL':
                    worksheet.set_column(col_num, col_num, width, text_format)
                elif col_name in wrap_columns:
                    worksheet.set_column(col_num, col_num, width, wrap_format)
                else:
                    # 对于其他列，只设置宽度和顶部对齐
                    worksheet.set_column(col_num, col_num, width, top_align_format)

            # 设置行高，确保多行文本能完全显示
            for row_num in range(len(df)):
                max_lines = 1
                for col_name in wrap_columns:
                    if col_name in df.columns:
                        cell_value = str(df.iloc[row_num][col_name])
                        num_lines = cell_value.count('\n') + 1
                        max_lines = max(max_lines, num_lines)

                if max_lines > 1:
                    worksheet.set_row(row_num + 1, max_lines * 15, wrap_format)
                else:
                    worksheet.set_row(row_num + 1, None, top_align_format)

        cs_console.print(
            f"    [green]Success:[/green] Quake Excel 已保存: '{os.path.basename(excel_path)}' ({len(df)} 条)")
    except Exception as e:
        logging.error(f"保存格式化的Quake Excel失败 ({excel_path}): {e}", exc_info=True)
        cs_console.print(f"    [bold red]Error:[/bold red] 保存格式化的Quake Excel失败: {os.path.basename(excel_path)}")


def write_fofa_results_to_excel(output_dir, target_name, data, stage="fofa_reverse_lookup"):
    if not data: return
    filename_suffix = generate_filename_suffix(target_name, stage)
    excel_path = os.path.join(output_dir, f"fofa_results{filename_suffix}.xlsx")
    try:
        pd.DataFrame(data).to_excel(excel_path, index=False, sheet_name="Fofa_Reverse_Lookup")
        cs_console.print(
            f"    [green]Success:[/green] Fofa反查Excel已保存: '{os.path.basename(excel_path)}' ({len(data)} 条)")
    except Exception as e:
        logging.error(f"保存Fofa反查Excel失败 ({excel_path}): {e}", exc_info=True)


def write_icp_assets_to_excel(output_dir, company_name, data):
    """
    [重构] 将通过 wer.plus 查询到的备案资产 (web, app, mapp) 写入Excel。
    """
    if not data: return
    filename_suffix = generate_filename_suffix(company_name, "icp_assets")
    excel_path = os.path.join(output_dir, f"icp_assets_results{filename_suffix}.xlsx")
    try:
        pd.DataFrame(data).to_excel(excel_path, index=False, sheet_name="ICP_Assets_Data")
        cs_console.print(
            f"      [green]Success:[/green] 备案资产 (Web/APP) Excel已保存: '{os.path.basename(excel_path)}' ({len(data)} 条)")
    except Exception as e:
        logging.error(f"保存备案资产 (Web/APP) Excel失败: {e}")


def write_summary_app_report_to_excel(output_dir, target_name, all_data):
    if not all_data: return
    filename_suffix = generate_filename_suffix(target_name, "app_summary")
    excel_path = os.path.join(output_dir, f"app_summary{filename_suffix}.xlsx")
    try:
        pd.DataFrame(all_data).to_excel(excel_path, index=False, sheet_name="All_Apps_Summary")
        cs_console.print(
            f"    [green]Success:[/green] 小程序/APP汇总报告已保存: '{os.path.basename(excel_path)}' (共 {len(all_data)} 条)")
    except Exception as e:
        logging.error(f"保存小程序/APP汇总报告失败: {e}")


def write_final_summary_report(output_base_dir, all_data):
    if not all_data: return
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    excel_path = os.path.join(output_base_dir, f"FINAL_APP_SUMMARY_{timestamp}.xlsx")
    try:
        pd.DataFrame(all_data).to_excel(excel_path, index=False, sheet_name="Final_All_Apps_Summary")
        cs_console.print(
            f"    [green]Success:[/green] 最终汇总报告已保存: '{os.path.basename(excel_path)}' (共 {len(all_data)} 条)")
    except Exception as e:
        logging.error(f"保存最终小程序/APP汇总报告失败: {e}")


def write_favicon_summary_to_excel(output_dir: str, all_favicon_summaries: list):
    """
    [重构版] 将所有目标的Favicon汇总数据进行最终的全局去重和合并，并输出到Excel。
    确保每个主体单位只出现一次，其查询语句包含所有不重复的favicon hash。
    """
    if not all_favicon_summaries:
        cs_console.print("\n[yellow]INFO:[/yellow] 未收集到任何Favicon信息，不生成关联语句报告。")
        return

    cs_console.print(f"\n[bold blue]开始生成全局Favicon关联语句报告...[/bold blue]")

    # --- 核心逻辑：使用defaultdict(set)进行聚合与去重 ---
    # 1. 收集和去重
    #    - 键: 目标单位名称
    #    - 值: 一个包含所有不重复favicon hash的集合(set)
    final_favicon_map = defaultdict(set)
    for summary_item in all_favicon_summaries:
        company = summary_item.get("目标单位")
        # 从 "favicon: "hash1" OR favicon: "hash2"" 这样的字符串中提取出所有的hash
        hashes = re.findall(r'favicon:\s*"(.*?)"', summary_item.get("favicon关联语句", ""))
        if company and hashes:
            for h in hashes:
                final_favicon_map[company].add(h)

    # 2. 为每个单位生成合并后的查询语句
    excel_data = []
    # 按单位名称排序，保证输出稳定
    for company, unique_hashes in sorted(final_favicon_map.items()):
        if unique_hashes:
            # 将集合排序以保证输出顺序稳定，然后拼接成最终的 OR 查询语句
            query_string = " OR ".join([f'favicon:"{h}"' for h in sorted(list(unique_hashes))])
            excel_data.append({"目标单位": company, "全局Favicon关联语句": query_string})

    # 3. 保存到Excel
    if excel_data:
        try:
            df = pd.DataFrame(excel_data)
            output_path = os.path.join(output_dir, "Favicon_Query_Summary_Global.xlsx")
            df.to_excel(output_path, index=False, engine="openpyxl")
            cs_console.print(
                f"  [green]Success:[/green] 全局Favicon报告已保存: '{os.path.basename(output_path)}' (共 {len(df)} 条)")
        except Exception as e:
            logging.error(f"保存全局Favicon报告失败: {e}", exc_info=True)
            cs_console.print(f"  [bold red]Error:[/bold red] 保存全局Favicon报告失败。")


def run_report_only_mode(db_conn):
    """
    运行一个快速的报告生成模式，仅创建自查报告Excel文件。
    """
    cs_console.print(f"[bold blue]仅生成自查报告模式启动...[/bold blue]")
    # 在此模式下，没有实际的扫描任务，所以失败列表为空
    failed_targets_list = []
    create_self_check_report(failed_targets_list, db_conn, "report_only")


def create_self_check_report(failed_targets_list, db_conn, mode_name):
    if not failed_targets_list and not db_conn: return
    report_path = os.path.join(OUTPUT_BASE_DIR, "自查报告.xlsx")
    cs_console.print(f"\n[bold blue]生成自查报告...[/bold blue] -> '{report_path}'")
    try:
        valid_cached_targets_for_excel = []
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT target_id, target_name, last_queried_quake FROM Targets WHERE last_queried_quake IS NOT NULL")
        for target_id, target_name, timestamp_str in cursor.fetchall():
            try:
                last_queried_dt = datetime.datetime.fromisoformat(timestamp_str)
                cache_age_hours = (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600
                if cache_age_hours < CACHE_EXPIRY_HOURS:
                    remaining_hours = round(CACHE_EXPIRY_HOURS - cache_age_hours, 2)
                    found_companies = set()
                    cursor.execute("SELECT raw_json FROM QuakeRawData WHERE target_id = ?", (target_id,))
                    for (raw_json_str,) in cursor.fetchall():
                        unit_name = json.loads(raw_json_str).get("service", {}).get("http", {}).get("icp", {}).get(
                            "main_licence", {}).get("unit", "")
                        if unit_name: found_companies.add(unit_name)
                    companies_str = "\n".join(sorted(list(found_companies))) or "未发现主体单位"
                    valid_cached_targets_for_excel.append({'查询目标': target_name, '包含的备案主体': companies_str,
                                                           '缓存时间': timestamp_str.split('.')[0],
                                                           '剩余有效期(小时)': remaining_hours})
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            if failed_targets_list:
                pd.DataFrame(failed_targets_list).rename(columns={'name': '查询目标', 'reason': '原因'}).to_excel(
                    writer, sheet_name="未正确查询的目标", index=False)
            else:
                pd.DataFrame([{'状态': '本次运行所有目标的Quake数据均已成功获取'}]).to_excel(writer,
                                                                                             sheet_name="未正确查询的目标",
                                                                                             index=False)
            if valid_cached_targets_for_excel:
                pd.DataFrame(valid_cached_targets_for_excel).to_excel(writer, sheet_name="有效期内的缓存目标",
                                                                      index=False)
            else:
                pd.DataFrame([{'状态': '当前数据库中无有效缓存'}]).to_excel(writer, sheet_name="有效期内的缓存目标",
                                                                            index=False)
        cs_console.print(f"  [green]Success:[/green] 自查报告已生成。")
    except Exception as e:
        logging.error(f"生成自查报告失败: {e}", exc_info=True)


def write_ips_to_file(output_dir, company_name, ip_list, stage=""):
    filename_suffix = generate_filename_suffix(company_name, stage)
    ip_list_file = os.path.join(output_dir, f"ips{filename_suffix}.txt")
    valid_ips = {ip for ip in ip_list if isinstance(ip, str) and ip.count('.') == 3}
    if not valid_ips: return None
    sorted_ips = sorted(list(valid_ips))
    try:
        with open(ip_list_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted_ips))
        cs_console.print(
            f"    [green]Success:[/green] IP列表已保存: '{os.path.basename(ip_list_file)}' ({len(sorted_ips)} 条)")
        return ip_list_file
    except Exception as e:
        logging.error(f"保存IP列表文件失败 ({ip_list_file}): {e}")
        return None


def write_urls_to_txt_file(output_dir, name_for_file, url_list, stage=""):
    filename_suffix = generate_filename_suffix(name_for_file, stage)
    url_list_file = os.path.join(output_dir, f"extracted_urls{filename_suffix}.txt")
    valid_urls = {str(u).strip() for u in url_list if u and str(u).strip()}
    if not valid_urls: return None
    sorted_urls = sorted(list(valid_urls))
    try:
        with open(url_list_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted_urls))
        cs_console.print(
            f"    [green]Success:[/green] URL列表已保存: '{os.path.basename(url_list_file)}' ({len(sorted_urls)} 条)")
        return url_list_file
    except Exception as e:
        logging.error(f"保存URL列表文件失败 ({url_list_file}): {e}")
        return None


def process_all_generated_csvs(output_base_dir_param):
    cs_console.print(f"\n[green]INFO:[/green] 开始最终的CSV到Excel批量转换...")
    for dirpath, _, filenames in os.walk(output_base_dir_param):
        for filename in [f for f in filenames if f.endswith(".csv")]:
            csv_file_path = os.path.join(dirpath, filename)
            excel_file_path = os.path.splitext(csv_file_path)[0] + ".xlsx"
            try:
                try:
                    df_csv = pd.read_csv(csv_file_path, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    df_csv = pd.read_csv(csv_file_path, encoding='gbk')
                if not df_csv.empty:
                    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                        df_csv.to_excel(writer, sheet_name="原始数据", index=False)
                        if 'status_code' in df_csv.columns:
                            df_copy = df_csv.copy()
                            df_copy['status_code'] = pd.to_numeric(df_copy['status_code'], errors='coerce').fillna(
                                0).astype(int)
                            df_valid = df_copy[df_copy['status_code'].isin([200, 301, 302])]
                            df_invalid = df_copy[~df_copy['status_code'].isin([200, 301, 302])]
                            if not df_valid.empty: df_valid.to_excel(writer, sheet_name="有效表", index=False)
                            if not df_invalid.empty: df_invalid.to_excel(writer, sheet_name="无效表", index=False)
                os.remove(csv_file_path)
            except Exception as e:
                logging.error(f"处理CSV文件 {csv_file_path} 失败: {e}", exc_info=True)


# ======================= 外部工具调用与处理 =======================
def archive_intermediate_files(company_dir_path, company_name_for_log):
    related_materials_dir = os.path.join(company_dir_path, "related_materials")
    os.makedirs(related_materials_dir, exist_ok=True)
    moved_files_count = 0
    if os.path.exists(company_dir_path):
        for filename in os.listdir(company_dir_path):
            if filename.endswith((".txt", ".json")):
                source_path = os.path.join(company_dir_path, filename)
                if os.path.isfile(source_path):
                    try:
                        shutil.move(source_path, os.path.join(related_materials_dir, filename))
                        moved_files_count += 1
                    except Exception as e:
                        logging.error(f"移动文件 '{filename}' 失败: {e}")
    if moved_files_count > 0:
        cs_console.print(f"    [green]整理:[/green] 已将 {moved_files_count} 个临时文件归档到 'related_materials'。")


def run_observer_ward(company_name, company_dir_path, urls_to_fingerprint, stage=""):
    """
    [最终加固版] 调用 observer_ward 进行URL指纹识别。
    - 核心修正：在调用程序前，强制确保输出文件所在的目录已存在，以修复 observer_ward 自身无法创建目录的Bug。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_dir = os.path.join(script_dir, 'tools')
    observer_ward_path = os.path.join(tools_dir, 'observer_ward.exe')
    if not os.path.exists(observer_ward_path):
        cs_console.print(f"  [bold red]Error:[/bold red] observer_ward.exe 未找到，跳过指纹识别。")
        return None

    # 1. 准备输入文件 (逻辑不变)
    input_file = write_urls_to_txt_file(company_dir_path, company_name, urls_to_fingerprint, f"observer_input_{stage}")
    if not input_file:
        return None

    # 2. 准备输出文件路径
    output_file = os.path.join(company_dir_path, f"url_fingerprint{generate_filename_suffix(company_name, stage)}.csv")

    # 3. 【核心修正】在调用子进程之前，确保输出文件所在的目录真实存在。
    #    这可以避免 observer_ward 因找不到路径而崩溃。
    output_directory = os.path.dirname(output_file)
    os.makedirs(output_directory, exist_ok=True)

    # 4. 构建并执行命令
    command = [observer_ward_path, '-l', input_file, '-o', output_file]
    if not SHOW_SCAN_INFO:
        command.append('--silent')

    cs_console.print(f"    [blue]执行:[/blue] observer_ward URL指纹识别 ({stage})...")
    try:
        # 将工作目录(cwd)设置为包含 observer_ward.exe 的目录
        subprocess.run(command, check=True, cwd=tools_dir, capture_output=not SHOW_SCAN_INFO, text=True,
                       encoding='utf-8', errors='ignore')
        cs_console.print(f"      [green]Success:[/green] 指纹识别结果已保存: '{os.path.basename(output_file)}'")
        return output_file
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        # 从异常对象中获取更详细的错误输出
        error_message = e.stderr if hasattr(e, 'stderr') and e.stderr else str(e)
        logging.error(f"observer_ward 执行失败: {error_message}", exc_info=True)
        cs_console.print(f"      [bold red]Error:[/bold red] observer_ward 执行失败 (详情见日志)。")
        return None


# ======================= Fofa反查封装模块 =======================
def handle_fofa_reverse_lookup(target_name, target_id, target_dir, raw_quake_data, db_conn, args):
    """
    封装了完整的Fofa IP反查逻辑，包括获取IP、调用API、处理缓存、保存结果等。
    (重构版：确保干净IP和被过滤IP都存为文件)
    """
    no_fofa = args.no_fofa
    skip_fofa_fingerprint = args.skip_fofa_fingerprint

    if no_fofa:
        return

    cs_console.print(f"\n[bold blue]>>>>>> 开始对目标 '{target_name}' 进行Fofa IP反查 <<<<<<[/bold blue]")

    all_ips = {item.get("ip") for item in raw_quake_data if item.get("ip")}
    if not all_ips:
        cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 未发现任何IP，跳过Fofa反查。")
        return

    # [核心修改] 将原始数据传递给过滤函数
    fofa_target_ips, filtered_out_ips = identify_shared_service_ips(raw_quake_data)
    fofa_output_dir = os.path.join(target_dir, "fofa_results")
    os.makedirs(fofa_output_dir, exist_ok=True)

    if filtered_out_ips:
        filtered_ip_file = write_ips_to_file(fofa_output_dir, target_name, filtered_out_ips, "filtered_ips_for_fofa")
        if filtered_ip_file:
            cs_console.print(f"      - [dim]被过滤的共享IP已保存到 '{os.path.basename(filtered_ip_file)}'[/dim]")

    if fofa_target_ips:
        clean_ip_file = write_ips_to_file(fofa_output_dir, target_name, fofa_target_ips, "clean_ips_for_fofa")
        if clean_ip_file:
            cs_console.print(f"      - [dim]用于Fofa查询的干净IP已保存到 '{os.path.basename(clean_ip_file)}'[/dim]")

    if fofa_target_ips:
        cs_console.print(f"    [blue]执行:[/blue] 将对过滤后的 {len(fofa_target_ips)} 个独立IP进行Fofa反查。")

        fofa_parsed_data = check_and_get_fofa_cache(target_id, db_conn)
        if fofa_parsed_data is None:
            fofa_raw_data, _ = query_fofa_by_ips(fofa_target_ips, target_id, db_conn)
            fofa_parsed_data = parse_fofa_results(fofa_raw_data) if fofa_raw_data else []

        if fofa_parsed_data:
            write_fofa_results_to_excel(fofa_output_dir, target_name, fofa_parsed_data)
            if not skip_fofa_fingerprint:
                fofa_urls = [item["URL"] for item in fofa_parsed_data if
                             item.get("URL", "").lower().startswith(('http://', 'https://'))]
                if fofa_urls:
                    run_observer_ward(target_name, fofa_output_dir, fofa_urls, stage="fingerprint_from_fofa")
    else:
        cs_console.print("      [yellow]INFO:[/yellow] 过滤后无独立IP可用于Fofa反查。")


# ======================= Host碰撞模块 =======================
SIMILARITY_THRESHOLD = 0.95

# 抑制InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 定义默认页面特征
DEFAULT_PAGE_TITLES = [
    "welcome to nginx", "welcome to tengine", "welcome to openresty",
    "apache http server test page", "it works!", "iis windows server",
    "centos test page", "red hat test page", "ubuntu test page"
]


# --- 新增：内容清洗函数 ---
def clean_html(html_content):
    """
    清洗HTML内容，去除脚本、样式、注释等动态或无关紧要的部分，以便进行更准确的内容比较。
    """
    if not html_content:
        return ""
    # 移除<script>标签及其内容
    cleaned_text = re.sub(r'<script\b[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    # 移除<style>标签及其内容
    cleaned_text = re.sub(r'<style\b[^>]*>.*?</style>', '', cleaned_text, flags=re.DOTALL | re.IGNORECASE)
    # 移除HTML注释
    cleaned_text = re.sub(r'', '', cleaned_text, flags=re.DOTALL)
    # 移除HTML标签，只保留文本内容
    cleaned_text = re.sub(r'<[^>]+>', '', cleaned_text)
    # 移除空白行和多余的空格
    cleaned_text = '\n'.join(line.strip() for line in cleaned_text.split('\n') if line.strip())


def is_ip_address(hostname):
    """
    检查一个给定的字符串是否是有效的IP地址 (IPv4或IPv6)。
    """
    if not hostname:
        return False
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        return False


def parse_fingerprint_for_collision(report_path):
    """解析observer_ward的报告，提取需要进行碰撞的主机名+端口。"""
    if not report_path or not os.path.exists(report_path):
        return set()

    hosts_to_test = set()
    try:
        # 尝试用utf-8-sig读取，失败则用gbk
        try:
            df = pd.read_csv(report_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            df = pd.read_csv(report_path, encoding='gbk')

        required_cols = {'url', 'status_code', 'title'}
        if not required_cols.issubset(df.columns):
            cs_console.print(
                f"    [yellow]Warning:[/yellow] 指纹报告 '{os.path.basename(report_path)}' 缺少必要列，无法提取碰撞目标。")
            return set()

        for _, row in df.iterrows():
            status = int(row['status_code'])
            title = str(row['title']).lower().strip()

            should_add = False

            # host碰撞提取规则如下：
            # 规则1: 状态码为 4xx 或 5xx（未正常访问站点）
            if 400 <= status < 600:
                should_add = True
            # 规则2: 标题匹配默认页面
            elif any(default_title in title for default_title in DEFAULT_PAGE_TITLES):
                should_add = True

            if should_add:
                try:
                    parsed_url = urlparse(row['url'])
                    hostname = parsed_url.hostname
                    port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
                    if hostname and not is_ip_address(hostname):
                        hosts_to_test.add(f"{hostname}:{port}")
                except Exception:
                    continue
    except Exception as e:
        logging.error(f"解析指纹报告 '{report_path}' 失败: {e}")
        return set()

    cs_console.print(f"    [blue]INFO:[/blue] 从指纹报告中提取了 {len(hosts_to_test)} 个需要碰撞的主机名。")
    return list(hosts_to_test)


def get_baseline(ip, port):
    """获取IP:Port的基线响应，返回包含状态和完整内容的字典。"""
    try:
        scheme = 'https' if port == 443 else 'http'
        url = f"{scheme}://{ip}:{port}"
        headers = {'Host': f"{ip}.{int(time.time())}.invalid", 'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=5, verify=False, allow_redirects=False)
        return {'status': response.status_code, 'content': response.text}
    except requests.exceptions.RequestException:
        return {'status': -1, 'content': ""}


def check_host_collision(ip, port, hostname, baseline_response):
    """执行单次碰撞，并使用“状态码+相似度”双重验证逻辑进行比对。"""
    try:
        scheme = 'https' if port == 443 else 'http'
        url = f"{scheme}://{ip}:{port}"
        headers = {'Host': hostname, 'User-Agent': 'Mozilla/5.0'}

        collision_response_obj = requests.get(url, headers=headers, timeout=5, verify=False, allow_redirects=False)

        baseline_status = baseline_response.get('status', -1)
        collision_status = collision_response_obj.status_code

        is_success = False
        similarity_ratio = 1.0

        # 双重验证逻辑
        baseline_is_ok = 200 <= baseline_status < 400
        collision_is_ok = 200 <= collision_status < 400
        if baseline_is_ok != collision_is_ok:
            is_success = True
        else:
            baseline_content = baseline_response.get('content', '')
            collision_content = collision_response_obj.text

            cleaned_baseline = clean_html(baseline_content)
            cleaned_collision = clean_html(collision_content)

            if not cleaned_collision: return None

            similarity_ratio = SequenceMatcher(None, cleaned_baseline, cleaned_collision).ratio()

            if similarity_ratio < SIMILARITY_THRESHOLD:
                is_success = True

        if is_success:
            collision_content = collision_response_obj.text
            title_match = re.search(r'<title>(.*?)</title>', collision_content, re.IGNORECASE | re.DOTALL)
            title = title_match.group(1).strip().lower() if title_match else ""

            if any(default_title in title for default_title in DEFAULT_PAGE_TITLES):
                return None

            return {
                'ip': ip,
                'port': port,
                'found_host': hostname,
                'status_code': collision_status,
                'similarity': f"{similarity_ratio:.2%}",
                'title': title.replace("\n", " ").replace("\r", " ").strip()
            }

    except requests.exceptions.RequestException:
        return None
    return None


def run_host_collision_orchestrator(ip_pool, hosts_to_test):
    """主编排函数，已更新为使用新的双重验证逻辑和精确统计。"""
    cs_console.print(f"    [bold blue]>>> 开始Host碰撞检测 (v3.1 双重验证模式)...[/bold blue]")
    if not ip_pool or not hosts_to_test:
        cs_console.print("        [yellow]Warning:[/yellow] IP池或“弹药库”为空，跳过碰撞。")
        return []

    tasks_to_create = []
    for host_port_str in hosts_to_test:
        try:
            hostname, port_str = host_port_str.rsplit(':', 1)
            port = int(port_str)
            for ip in ip_pool:
                tasks_to_create.append({'ip': ip, 'port': port, 'hostname': hostname})
        except ValueError:
            hostname = host_port_str
            for ip in ip_pool:
                tasks_to_create.append({'ip': ip, 'port': 80, 'hostname': hostname})
                tasks_to_create.append({'ip': ip, 'port': 443, 'hostname': hostname})

    if not tasks_to_create:
        return []

    found_assets = []
    required_baselines = {(task['ip'], task['port']) for task in tasks_to_create}
    with ThreadPoolExecutor(max_workers=50) as executor:
        baselines = {}
        future_to_ip_port = {executor.submit(get_baseline, ip, port): (ip, port) for ip, port in required_baselines}
        cs_console.print(f"        [dim]正在获取 {len(required_baselines)} 个IP:Port组合的基线响应 (存活探测)...[/dim]")
        for future in as_completed(future_to_ip_port):
            ip, port = future_to_ip_port[future]
            baselines[f"{ip}:{port}"] = future.result()

        collision_tasks_params = []
        for task_params in tasks_to_create:
            ip, port = task_params['ip'], task_params['port']
            baseline_response = baselines.get(f"{ip}:{port}")
            if baseline_response and baseline_response['status'] != -1:
                collision_tasks_params.append(task_params)

        failed_baseline_count = len(required_baselines) - (
                len(baselines) - sum(1 for r in baselines.values() if r['status'] == -1))
        cs_console.print(f"    [bold cyan]-- 基线探测(存活检查)结果 --[/bold cyan]")
        cs_console.print(f"    ├── [dim]尝试连接IP:Port组合:[/dim] [bold]{len(required_baselines)}[/bold] 个")
        cs_console.print(f"    ├── [dim]连接失败/无Web服务:[/dim] [bold red]{failed_baseline_count}[/bold red] 个")
        cs_console.print(
            f"    └── [dim]最终创建有效碰撞任务:[/dim] [bold green]{len(collision_tasks_params)}[/bold green] 个")

        if not collision_tasks_params:
            cs_console.print(f"\n[bold blue]<<< Host碰撞检测完成，无存活目标可碰撞。[/bold blue]")
            return []

        future_tasks = {executor.submit(check_host_collision, t['ip'], t['port'], t['hostname'],
                                        baselines.get(f"{t['ip']}:{t['port']}")): t for t in collision_tasks_params}

        with Progress(console=cs_console) as progress:
            p_task = progress.add_task("[cyan]碰撞中...", total=len(future_tasks))
            for future in as_completed(future_tasks):
                result = future.result()
                if result:
                    progress.stop()
                    cs_console.print(
                        f"        [bold green]碰撞成功![/bold green] IP: {result['ip']}:{result['port']}, Host: {result['found_host']}, Status: {result['status_code']}, Sim: {result['similarity']}")
                    progress.start()
                    found_assets.append(result)
                progress.update(p_task, advance=1)

    cs_console.print(f"\n[bold blue]<<< Host碰撞检测完成，共发现 {len(found_assets)} 个潜在资产。[/bold blue]")
    return found_assets


def write_collision_results_to_excel(output_dir, company_name, data, stage="host_collision"):
    """保存Host碰撞成功的结果到Excel，增加新列。"""
    if not data:
        return
    filename_suffix = generate_filename_suffix(company_name, stage)
    excel_path = os.path.join(output_dir, f"collision_results{filename_suffix}.xlsx")
    try:
        df = pd.DataFrame(data)
        df = df[['ip', 'port', 'found_host', 'status_code', 'similarity', 'title']]
        df.to_excel(excel_path, index=False, sheet_name="Host_Collision_Finds")
        cs_console.print(
            f"    [green]Success:[/green] Host碰撞结果已保存: '{os.path.basename(excel_path)}' ({len(data)} 条)")
    except Exception as e:
        logging.error(f"保存Host碰撞Excel失败 ({excel_path}): {e}", exc_info=True)


def execute_host_collision_workflow(args, fingerprint_report_path, initial_urls, company_dir, company_name,
                                    company_raw_quake_data):
    """
    封装了完整的Host碰撞工作流，从解析报告到执行碰撞再到保存结果。
    (重构版：确保干净IP和被过滤IP都存为文件)
    """
    if not fingerprint_report_path:
        cs_console.print("    [yellow]Warning:[/yellow] 未生成有效的指纹报告，跳过Host碰撞。")
        return

    cs_console.print("    [blue]INFO:[/blue] 开始计算连接失败的URL用于Host碰撞...")
    initial_urls_set = set(initial_urls)
    hosts_from_failed_connections = set()
    try:
        try:
            df_report = pd.read_csv(fingerprint_report_path, usecols=['url'], encoding='utf-8-sig')
        except UnicodeDecodeError:
            df_report = pd.read_csv(fingerprint_report_path, usecols=['url'], encoding='gbk')

        reported_urls_set = set(df_report['url'])
        connection_failed_urls = initial_urls_set - reported_urls_set

        for url in connection_failed_urls:
            try:
                parsed_url = urlparse(url)
                hostname = parsed_url.hostname
                port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
                if hostname and not is_ip_address(hostname):
                    hosts_from_failed_connections.add(f"{hostname}:{port}")
            except Exception:
                continue
        cs_console.print(f"    [blue]INFO:[/blue] 发现 {len(hosts_from_failed_connections)} 个连接失败的目标。")

    except Exception as e:
        cs_console.print(f"    [yellow]Warning:[/yellow] 计算URL差集时出错: {e}")

    hosts_from_report_analysis = set(parse_fingerprint_for_collision(fingerprint_report_path))
    hosts_to_test = list(hosts_from_failed_connections | hosts_from_report_analysis)

    if hosts_to_test:
        write_urls_to_txt_file(company_dir, company_name, sorted(hosts_to_test), "collision_ammunition_final")

        # [核心修改] 将原始数据传递给过滤函数
        collision_ip_pool, filtered_out_ips = identify_shared_service_ips(company_raw_quake_data)

        if collision_ip_pool:
            clean_ip_file = write_ips_to_file(company_dir, company_name, collision_ip_pool, "clean_ips_for_collision")
            if clean_ip_file:
                cs_console.print(f"    [dim]用于Host碰撞的干净IP已保存到 '{os.path.basename(clean_ip_file)}'[/dim]")

        if filtered_out_ips:
            filtered_ip_file = write_ips_to_file(company_dir, company_name, filtered_out_ips,
                                                 "filtered_ips_for_collision")
            if filtered_ip_file:
                cs_console.print(f"    [dim]Host碰撞前被过滤的IP已保存到 '{os.path.basename(filtered_ip_file)}'[/dim]")

        collision_results = run_host_collision_orchestrator(collision_ip_pool, hosts_to_test)
        if collision_results:
            write_collision_results_to_excel(company_dir, company_name, collision_results)
    else:
        cs_console.print("    [yellow]Warning:[/yellow] 未能从任何规则中提取到Host碰撞的目标。")


# ======================= 高级模式专属函数 =======================
def run_gogo_scan(company_name, iplist_file_path, port_list, company_dir_path):
    global SHOW_SCAN_INFO
    cs_console.print(f"    [blue]执行:[/blue] Gogo 主动扫描...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_dir = os.path.join(script_dir, 'tools')
    gogo_exe_path = os.path.join(tools_dir, 'gogo.exe')
    if not os.path.exists(gogo_exe_path):
        cs_console.print(f"    [bold red]Error:[/bold red] gogo.exe 未找到于 '{tools_dir}'.")
        return None
    filename_suffix = generate_filename_suffix(company_name, "gogo_scan")
    absolute_output_file = os.path.join(company_dir_path, f"gogo_results{filename_suffix}.json")
    ports_str = ",".join(map(str, sorted(list(set(port_list)), key=int)))
    if not ports_str:
        cs_console.print(f"    [yellow]Warning:[/yellow] 没有提供有效端口给gogo，跳过扫描。")
        return None
    command = [gogo_exe_path, '-l', iplist_file_path, '-p', ports_str, '-v', '-C', '-t', '1000', '-O', 'jl', '-f',
               absolute_output_file]
    if not SHOW_SCAN_INFO: command.append('-q')
    logging.info(f"执行 gogo 命令: {' '.join(command)}")
    try:
        subprocess_kwargs = {"check": True, "cwd": tools_dir}
        if not SHOW_SCAN_INFO:
            subprocess_kwargs.update({"capture_output": True, "text": True, "encoding": 'utf-8', "errors": 'ignore'})
        subprocess.run(command, **subprocess_kwargs)
        cs_console.print(
            f"      [green]Success:[/green] gogo扫描完成, 结果保存在: '{os.path.basename(absolute_output_file)}'")
        return absolute_output_file
    except (subprocess.CalledProcessError, Exception) as e:
        logging.error(f"gogo 执行出错: {e}", exc_info=True)
        cs_console.print(f"      [bold red]Error:[/bold red] gogo 执行出错 (详情见日志)。")
        return None


def process_gogo_output_and_generate_excel(gogo_output_path, company_name, company_dir_path):
    """
    (xlsxwriter多Sheet+格式化最终版) 解析Gogo报告，生成多Sheet的Excel，并自动设置换行和列宽。
    """
    if not gogo_output_path or not os.path.exists(gogo_output_path) or os.path.getsize(gogo_output_path) == 0:
        return []

    # 1. 解析原始数据
    host_results = []
    try:
        with open(gogo_output_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    host_results.append(json.loads(line.strip()))
    except Exception as e:
        logging.error(f"解析gogo结果文件 '{gogo_output_path}' 失败: {e}", exc_info=True)
        return []

    if not host_results:
        cs_console.print(f"    [yellow]INFO:[/yellow] gogo扫描未发现有效资产记录。")
        return []

    # 2. 格式化数据
    final_table_rows, discovered_urls = [], set()
    cs_console.print(f"    [blue]处理:[/blue] 正在将 {len(host_results)} 条Gogo扫描结果格式化为Excel报告...")

    for result in host_results:
        # 健壮性检查：跳过非标准资产格式的行
        if not isinstance(result, dict) or not result.get("ip") or not result.get("port"):
            logging.warning(f"跳过非标准资产格式的Gogo结果: {result}")
            continue

        protocol, ip, port = result.get('protocol', '').lower(), result.get('ip', ''), str(result.get('port', ''))
        url = ''
        if protocol in ['http', 'https']:
            url = f"{protocol}://{ip}" + (f":{port}" if not (
                    (protocol == 'http' and port == '80') or (protocol == 'https' and port == '443')) else "")
            discovered_urls.add(url)

        vulns_string = '\n'.join(result.get('vulns', {}).keys())
        frameworks_data = result.get('frameworks', {})
        names = '\n'.join(frameworks_data.keys())
        versions = '\n'.join([d.get('attributes', {}).get('version', '') for d in frameworks_data.values()])
        vendors = '\n'.join([d.get('attributes', {}).get('vendor', '') for d in frameworks_data.values()])
        products = '\n'.join([d.get('attributes', {}).get('product', '') for d in frameworks_data.values()])

        row_data = {'url': url, 'ip': ip, 'port': port, 'protocol': result.get('protocol', ''),
                    'status': result.get('status', ''), 'host': result.get('host', ''),
                    'title / banner': result.get('title', ''), 'midware': result.get('midware', ''),
                    'finger_name': names, 'finger_version': versions, 'finger_vendor': vendors,
                    'finger_product': products, 'Vulnerabilities': vulns_string}
        final_table_rows.append(row_data)

    if not final_table_rows:
        cs_console.print(f"    [yellow]INFO:[/yellow] gogo扫描结果中未找到可供报告的有效资产。")
        return list(discovered_urls)

    # 3. 创建并分类DataFrame
    df_all = pd.DataFrame(final_table_rows)
    final_columns = ['url', 'ip', 'port', 'protocol', 'status', 'host', 'title / banner', 'midware', 'finger_name',
                     'finger_version', 'finger_vendor', 'finger_product', 'Vulnerabilities']
    df_all = df_all.reindex(columns=final_columns, fill_value='')

    df_all['status_str'] = df_all['status'].astype(str)
    valid_statuses = ['open', '200', '301', '302']
    df_valid = df_all[df_all['status_str'].isin(valid_statuses)].copy()
    df_invalid = df_all[~df_all['status_str'].isin(valid_statuses)].copy()

    df_all.drop(columns=['status_str'], inplace=True)
    if not df_valid.empty: df_valid.drop(columns=['status_str'], inplace=True)
    if not df_invalid.empty: df_invalid.drop(columns=['status_str'], inplace=True)

    # 4. 使用xlsxwriter引擎写入并格式化Excel
    excel_path = os.path.join(company_dir_path,
                              f"Gogo_Full_Report{generate_filename_suffix(company_name, 'gogo_report')}.xlsx")
    try:
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # 写入数据到各个Sheet
            df_all.to_excel(writer, sheet_name="原始表", index=False)
            if not df_valid.empty: df_valid.to_excel(writer, sheet_name="有效表", index=False)
            if not df_invalid.empty: df_invalid.to_excel(writer, sheet_name="无效表", index=False)

            workbook = writer.book
            wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
            top_align_format = workbook.add_format({'valign': 'top'})
            text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})

            # 定义需要自动换行的列
            wrap_columns = ['title / banner', 'finger_name', 'finger_version', 'finger_vendor', 'finger_product',
                            'Vulnerabilities']

            # 遍历所有生成的Sheet，并应用格式
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]

                # 根据Sheet名获取对应的DataFrame以确定列
                current_df = {"原始表": df_all, "有效表": df_valid, "无效表": df_invalid}.get(sheet_name)

                if current_df is not None and not current_df.empty:
                    # 1. 先设置列宽
                    for col_num, col_name in enumerate(current_df.columns):
                        # 计算合适的列宽
                        try:
                            max_len_content = current_df[col_name].astype(str).map(len).max()
                            max_len = max(max_len_content, len(col_name))
                        except (ValueError, TypeError):
                            max_len = len(col_name)

                        width = min(max_len + 5, 70)

                        # 应用格式
                        if col_name == 'url':
                            worksheet.set_column(col_num, col_num, width, text_format)
                        elif col_name in wrap_columns:
                            worksheet.set_column(col_num, col_num, width, wrap_format)
                        else:
                            worksheet.set_column(col_num, col_num, width)

                    # 2. 再设置行高
                    for row_num in range(len(current_df)):
                        max_lines = 1
                        for col_name in wrap_columns:
                            if col_name in current_df.columns:
                                cell_value = str(current_df.iloc[row_num][col_name])
                                num_lines = cell_value.count('\n') + 1
                                max_lines = max(max_lines, num_lines)

                        if max_lines > 1:
                            worksheet.set_row(row_num + 1, max_lines * 15, wrap_format)
                        else:
                            worksheet.set_row(row_num + 1, None, top_align_format)

        cs_console.print(
            f"      [green]Success:[/green] Gogo详细报告 (含格式化) 已生成: '{os.path.basename(excel_path)}'")
    except Exception as e:
        logging.error(f"保存Gogo多Sheet Excel报告失败: {e}", exc_info=True)

    return list(discovered_urls)


# ======================= 主逻辑 =======================
def run_only_quake_mode(db_conn, args):
    """
    [重构版] 仅查询Quake资产的主流程。
    - 已更新以支持生成最终的Favicon汇总报告。
    - 已更新以处理预检返回的'skipped'状态。
    """
    start_time_quake_only = time.time()
    cs_console.print(f"[bold blue]Quake-Only 模式启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    failed_targets, all_quake_assets, all_favicon_summaries = [], [], []

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]"
        )
        raw_quake_data = check_and_get_quake_cache(target_name, db_conn)

        if raw_quake_data is None:
            # 第一次查询
            raw_quake_data = query_all_pages(target_name, db_conn)

            # 如果首次查询失败或跳过，尝试重试一次
            if raw_quake_data is None or raw_quake_data == 'skipped':
                cs_console.print(f"      [yellow]首次查询失败或跳过，等待3秒后重试...[/yellow]")
                time.sleep(3)
                # 重试查询
                cs_console.print(f"      [red]重试开始！继续处理目标 '{target_name}'[/red]")
                raw_quake_data = query_all_pages(target_name, db_conn)

                # 检查重试结果
                if raw_quake_data is None or raw_quake_data == 'skipped':
                    cs_console.print(f"      [red]重试后仍失败，跳过目标 '{target_name}'[/red]")
                    # 可选：记录日志、写入失败列表等
                    continue  # 跳过后续处理，进入下一个 target
                else:
                    cs_console.print(f"      [green]重试成功！继续处理下一个目标[/green]")

        # ✅ 此处 raw_quake_data 要么来自缓存，要么来自首次/重试成功的查询
        # 在这里处理 raw_quake_data（解析、入库、输出等）
        # ...

        # --- 修改：处理新的返回值 ---
        if raw_quake_data == 'skipped':
            failed_targets.append({'name': target_name, 'reason': f'资产数量超出阈值({QUAKE_PREFLIGHT_LIMIT})被跳过'})
            continue  # 直接处理下一个目标
        if raw_quake_data is None or not raw_quake_data:
            reason = 'API查询过程失败或出错' if raw_quake_data is None else '查询成功但无结果'
            failed_targets.append({'name': target_name, 'reason': reason})
            continue
        # --- 修改结束 ---

        cs_console.print(f"  [green]数据获取完成:[/green] 开始解析 {len(raw_quake_data)} 条资产记录...")
        parsed_data_for_report, favicon_summary = parse_results(raw_quake_data, target_name)
        all_quake_assets.extend(parsed_data_for_report)
        if favicon_summary: all_favicon_summaries.extend(favicon_summary)

    if all_quake_assets:
        cs_console.print(f"\n[bold blue]开始生成Quake资产总报告...[/bold blue]")
        output_path = os.path.join(OUTPUT_BASE_DIR, "Quake_Only_Results_all.xlsx")
        try:
            df = pd.DataFrame(all_quake_assets)
            columns_to_remove = ['components', 'scan_urls', 'Host', 'favicon_hash']
            df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name="Quake_Data_Summary", index=False)
                workbook, worksheet = writer.book, writer.sheets["Quake_Data_Summary"]
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})
                top_align_format = workbook.add_format({'valign': 'top'})
                wrap_columns = ['产品指纹', '网站标题']
                for col_num, col_name in enumerate(df.columns):
                    max_len = max(df[col_name].astype(str).map(len).max(), len(col_name)) if not df.empty else len(
                        col_name)
                    width = min(max_len + 5, 60)
                    col_format = text_format if col_name == 'URL' else wrap_format if col_name in wrap_columns else top_align_format
                    worksheet.set_column(col_num, col_num, width, col_format)
                for row_num in range(len(df)):
                    max_lines = 1
                    for col_name in wrap_columns:
                        if col_name in df.columns: max_lines = max(max_lines,
                                                                   str(df.iloc[row_num][col_name]).count('\n') + 1)
                    if max_lines > 1: worksheet.set_row(row_num + 1, max_lines * 15)
            cs_console.print(f"  [green]Success:[/green] Quake资产总报告已保存到: '{output_path}' (共 {len(df)} 条)")
        except Exception as e:
            cs_console.print(f"  [bold red]Error:[/bold red] 写入总报告失败: {e}")

    # 调用新的全局去重函数
    write_favicon_summary_to_excel(OUTPUT_BASE_DIR, all_favicon_summaries)

    create_self_check_report(failed_targets, db_conn, "only_quake")
    cs_console.print(
        f"\n[bold green]Quake-Only 模式结束.[/bold green] 总耗时: {round(time.time() - start_time_quake_only, 2)} 秒.")


def _process_target_workflow(db_conn, args, mode_name, report_suffix, is_advanced=False):
    """
    [最终集成版] 驱动基础和高级模式的核心工作流函数 (第三方接口版本)。
    - 包含了独立的“查漏补缺”阶段，彻底解决因Quake无结果而遗漏ICP查询的问题。
    """
    # ==============================================================================
    # 阶段一：初始化和准备工作
    # ==============================================================================
    types_to_check = [t.strip().lower() for t in args.checkother.split(',')] if args.checkother else []
    start_time = time.time()
    cs_console.print(f"[bold blue]{mode_name}启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    failed_targets, grand_total_apps_list, grand_total_favicon_summary = [], [], []

    # ==============================================================================
    # 阶段二：主循环，逐一处理每个目标 (Quake驱动流程)
    # ==============================================================================
    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

        # --- 2.1 Quake资产获取 (含缓存和重试逻辑) ---
        raw_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if raw_quake_data is None:
            raw_quake_data = query_all_pages(target_name, db_conn)
            if raw_quake_data is None or raw_quake_data == 'skipped':
                cs_console.print(f"      [yellow]首次查询失败或跳过，等待3秒后重试...[/yellow]")
                time.sleep(3)
                cs_console.print(f"      [red]重试开始！继续处理目标 '{target_name}'[/red]")
                raw_quake_data = query_all_pages(target_name, db_conn)
                if raw_quake_data is None or raw_quake_data == 'skipped':
                    cs_console.print(f"      [red]重试后仍失败，跳过目标 '{target_name}'[/red]")
                    reason = 'API查询过程失败或出错(含重试)' if raw_quake_data is None else f'资产数量超出阈值({QUAKE_PREFLIGHT_LIMIT})被跳过'
                    failed_targets.append({'name': target_name, 'reason': reason})
                    continue

        # --- 2.2 Quake结果校验 ---
        if raw_quake_data == 'skipped':
            failed_targets.append({'name': target_name, 'reason': f'资产数量超出阈值({QUAKE_PREFLIGHT_LIMIT})被跳过'})
            continue
        if raw_quake_data is None or not raw_quake_data:
            reason = 'API查询过程失败或出错' if raw_quake_data is None else '查询成功但无结果'
            failed_targets.append({'name': target_name, 'reason': reason})
            # 注意：即使这里continue，后续的查漏补缺阶段依然会处理这个target_name
            continue

        # --- 2.3 资产解析与分组 ---
        parsed_quake_data, favicon_summary = parse_results(raw_quake_data, target_name)
        if favicon_summary: grand_total_favicon_summary.extend(favicon_summary)

        target_id = get_target_id_from_db(target_name, db_conn)
        target_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(target_name))
        os.makedirs(target_dir, exist_ok=True)
        assets_by_company = defaultdict(lambda: {"parsed_data": [], "raw_data": []})
        raw_data_map_with_id = {id(d): d for d in raw_quake_data}
        real_asset_count = len(raw_quake_data)
        for i, p_item in enumerate(parsed_quake_data):
            if i < real_asset_count: p_item['_raw_ref_id'] = id(raw_quake_data[i])
        # 您原始代码中，这个循环似乎是冗余的，但为保持逻辑一致暂时保留
        for item in parsed_quake_data:
            company_key = item.get("主体单位") or f"未知主体单位_{target_name}"
            assets_by_company[company_key]["parsed_data"].append(item)
            raw_ref_id = item.pop('_raw_ref_id', None)
            if raw_ref_id in raw_data_map_with_id: assets_by_company[company_key]["raw_data"].append(
                raw_data_map_with_id[raw_ref_id])
        total_companies = len(assets_by_company)
        cs_console.print(f"  [green]Quake数据处理完成:[/green] 发现 {total_companies} 个主体单位。")

        # --- 2.4 循环处理每个主体单位（公司） ---
        for company_index, (company_name, assets) in enumerate(assets_by_company.items(), 1):
            cs_console.print(f"\n  ({company_index}/{total_companies}) 处理主体单位: [cyan]{company_name}[/cyan]")
            company_dir = os.path.join(target_dir, sanitize_sheet_name(company_name))
            os.makedirs(company_dir, exist_ok=True)
            write_quake_results_to_excel(company_dir, company_name, assets["parsed_data"], stage="quake")
            all_urls_for_fingerprint = [url for item in assets["parsed_data"] for url in item.get("scan_urls", []) if
                                        url]
            fingerprint_report_path = None
            if all_urls_for_fingerprint: fingerprint_report_path = run_observer_ward(company_name, company_dir,
                                                                                     all_urls_for_fingerprint,
                                                                                     stage="fingerprint_from_quake")
            if args.host_collision: execute_host_collision_workflow(args, fingerprint_report_path,
                                                                    all_urls_for_fingerprint, company_dir, company_name,
                                                                    assets["raw_data"])

            # --- 模式差异点：仅在高级模式下执行主动扫描 ---
            if is_advanced:
                gogo_urls_for_fingerprint = set()
                company_ips_list = list({item.get("IP") for item in assets["parsed_data"] if item.get("IP")})
                if company_ips_list:
                    ip_list_file = write_ips_to_file(company_dir, company_name, company_ips_list, "gogo_input")
                    if ip_list_file:
                        ports_to_scan = {int(item.get("Port")) for item in assets["parsed_data"] if
                                         item.get("Port")} | DEFAULT_PORTS
                        gogo_output_path = run_gogo_scan(company_name, ip_list_file, list(ports_to_scan), company_dir)
                        if gogo_output_path:
                            new_urls = process_gogo_output_and_generate_excel(gogo_output_path, company_name,
                                                                              company_dir)
                            if new_urls: gogo_urls_for_fingerprint.update(new_urls)
                if gogo_urls_for_fingerprint: run_observer_ward(company_name, company_dir,
                                                                list(gogo_urls_for_fingerprint),
                                                                stage="fingerprint_from_gogo")

            # --- 通用逻辑：查询ICP备案资产 ---
            if types_to_check and "未知主体" not in company_name:
                raw_icp_data = query_icp_assets_from_wer(company_name, db_conn, types_to_check)
                if raw_icp_data:
                    parsed_icp_data = parse_all_icp_results(raw_icp_data)
                    write_icp_assets_to_excel(company_dir, company_name, parsed_icp_data)
                    grand_total_apps_list.extend(parsed_icp_data)

            archive_intermediate_files(company_dir, company_name)

        # --- 2.5 Fofa IP反查 ---
        if target_id: handle_fofa_reverse_lookup(target_name, target_id, target_dir, raw_quake_data, db_conn, args)

    # ==============================================================================
    # 阶段三：“查漏补缺”，确保ICP备案报告的完整性
    # ==============================================================================
    if types_to_check:
        cs_console.print("\n[bold blue]>>> 开始最终一致性检查，确保所有目标的ICP备案信息均被查询...[/bold blue]")

        # 步骤1: 获取所有在主流程中已成功查询过ICP的主体单位
        successfully_queried_units = {item.get('备案主体') for item in grand_total_apps_list if item.get('备案主体')}

        # 步骤2: 计算出在输入目标中但其ICP信息从未被查询过的“遗漏目标”
        input_targets_set = set(target_names)
        missed_targets = input_targets_set - successfully_queried_units

        # 步骤3: 如果有遗漏，则为它们专门补全ICP查询
        if missed_targets:
            cs_console.print(
                f"    [yellow]INFO:[/yellow] 发现 {len(missed_targets)} 个目标在主流程中未覆盖ICP查询，开始强制补查...")
            for missed_target_name in sorted(list(missed_targets)):
                cs_console.print(f"\n  [cyan]强制补查目标: {missed_target_name}[/cyan]")
                # 直接调用ICP查询函数，不依赖Quake
                raw_icp_data_missed = query_icp_assets_from_wer(missed_target_name, db_conn, types_to_check)
                if raw_icp_data_missed:
                    parsed_icp_data_missed = parse_all_icp_results(raw_icp_data_missed)
                    if parsed_icp_data_missed:
                        grand_total_apps_list.extend(parsed_icp_data_missed)
                        cs_console.print(
                            f"    [green]补查成功:[/green] 已为 '{missed_target_name}' 新增 {len(parsed_icp_data_missed)} 条备案记录到总报告。")
                else:
                    cs_console.print(f"    [dim]补查完成:[/dim] '{missed_target_name}' 未发现相关ICP备案资产。")
            cs_console.print("\n[bold blue]<<< 所有遗漏目标的ICP备案补查已完成。[/bold blue]")
        else:
            cs_console.print("    [green]Success:[/green] 所有输入目标的ICP备案信息均已在主流程中覆盖，无需补查。")

    # ==============================================================================
    # 阶段四：收尾工作和最终报告生成
    # ==============================================================================
    if grand_total_apps_list:
        write_final_summary_report(OUTPUT_BASE_DIR, grand_total_apps_list)

    write_favicon_summary_to_excel(OUTPUT_BASE_DIR, grand_total_favicon_summary)
    process_all_generated_csvs(OUTPUT_BASE_DIR)
    create_self_check_report(failed_targets, db_conn, report_suffix)
    cs_console.print(f"\n[bold green]{mode_name}结束.[/bold green] 总耗时: {round(time.time() - start_time, 2)} 秒.")


def run_basic_mode(db_conn, args):
    """
    运行基础模式，调用核心工作流函数。
    """
    _process_target_workflow(
        db_conn=db_conn,
        args=args,
        mode_name="基础模式",
        report_suffix="basic",
        is_advanced=False
    )


def run_advanced_mode(db_conn, args):
    """
    运行高级模式，调用核心工作流函数并开启高级功能。
    """
    _process_target_workflow(
        db_conn=db_conn,
        args=args,
        mode_name="高级模式 (Gogo集成, 先被动后主动)",
        report_suffix="advanced_gogo",
        is_advanced=True
    )


def main():
    global SHOW_SCAN_INFO, INPUT_FILE, API_KEY, OUTPUT_BASE_DIR, FOFA_EMAIL, FOFA_KEY, WERPLUS_API_KEY

    parser = argparse.ArgumentParser(
        description="""

<───────────────────────────────────────────────────────────────────>

    ██╗ ██████╗██████╗   ICPAssetExpress
    ██║██╔════╝██╔══██╗  - 自动化ICP备案资产收集工具
    ██║██║     ██████╔╝
    ██║██║     ██╔═══╝   » Author : 123654mjx
    ██║╚██████╗██║       » 公众号 : 安服Log
    ╚═╝ ╚═════╝╚═╝

<───────────────────────────────────────────────────────────────────>
        """,
        formatter_class=argparse.RawTextHelpFormatter,
        usage=argparse.SUPPRESS,
        epilog=f"""
    使用示例:
      python {os.path.basename(__file__)} --onlyquake -i my_targets.txt -o E:\\results
      python {os.path.basename(__file__)} -a -i my_targets.txt -o C:\\my_scans --host-collision
      python {os.path.basename(__file__)} -b --apikey YOUR_KEY -checkother app,mapp -o D:\\outputs
      python {os.path.basename(__file__)} --onlyicp -i targets.txt -o E:\\icp_assets
      python {os.path.basename(__file__)} --report-only -o C:\\reports
    """
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--report-only', action='store_true', help="仅生成Excel自查报告，不执行任何扫描。")
    mode_group.add_argument('--onlyquake', action='store_true', help="仅查询Quake资产并输出表格，不进行任何主动扫描")
    mode_group.add_argument('-b', '--basic', action='store_true', help="运行基础模式")
    mode_group.add_argument('-a', '--advanced', action='store_true', help="运行高级模式 (使用gogo进行扫描, 默认)")
    mode_group.add_argument('--onlyicp', action='store_true', help="仅通过wer.plus查询备案资产 (web, app, mapp)")

    parser.add_argument('-i', '--input', type=str, help=f"指定输入文件名。默认为: '{INPUT_FILE}'。")
    parser.add_argument('-o', '--output', type=str, help="指定自定义的输出根目录 (必须是绝对路径)。")
    parser.add_argument('--apikey', type=str, help="指定360 Quake API Key。")
    parser.add_argument('--fofa-email', type=str, help="Fofa注册邮箱")
    parser.add_argument('--fofa-key', type=str, help="Fofa API Key")
    parser.add_argument('--werplus-key', type=str, help="wer.plus API Key")
    parser.add_argument('--showScanInfo', action='store_true', help="显示外部扫描工具的实时运行输出。")
    parser.add_argument('--skip-fofa-fingerprint', action='store_true', help="跳过对Fofa反查结果的URL进行指纹识别。")
    parser.add_argument('--no-fofa', action='store_true', help="完全跳过Fofa IP反查流程。")
    parser.add_argument('-checkother', type=str, help="查询额外信息，多个用逗号分隔 (app,mapp)。")
    parser.add_argument('--host-collision', action='store_true',
                        help="对识别出的异常/默认页面进行Host碰撞，发现隐藏资产。")

    args = parser.parse_args()

    # --- 核心修改：强制要求 -o 参数必须为绝对路径 ---
    # 检查 -o 参数是否提供
    if not args.output:
        cs_console.print("\n[bold red]错误:[/bold red] 您必须使用 -o 参数来指定一个输出目录。")
        cs_console.print("      [dim]这是为了确保外部工具能够正确定位文件，避免潜在的路径错误。[/dim]")
        parser.print_help()
        return

    # 检查 -o 参数是否为绝对路径
    if not os.path.isabs(args.output):
        cs_console.print(f"\n[bold red]错误:[/bold red] 为 -o 参数提供的路径必须是绝对路径。")
        cs_console.print(f"      [dim]您提供的相对路径是: '{args.output}'[/dim]")
        cs_console.print(f"      [dim]请提供一个完整的路径，例如: -o E:\\scan_results[/dim]")
        return
    # --- 检查结束 ---

    # --- 模式选择逻辑更新 ---
    if not any([args.report_only, args.onlyquake, args.basic, args.advanced, args.onlyicp]):
        args.advanced = True

    SHOW_SCAN_INFO = args.showScanInfo
    if args.input: INPUT_FILE = args.input
    # if args.apikey: API_KEY = args.apikey
    # if args.fofa_email: FOFA_EMAIL = args.fofa_email
    # if args.fofa_key: FOFA_KEY = args.fofa_key
    if args.werplus_key: WERPLUS_API_KEY = args.werplus_key

    # 根据选择的模式确定主函数和输出目录
    if args.report_only:
        mode_name = "report_only"
        chosen_mode_function = lambda db_conn: run_report_only_mode(db_conn)
    elif args.onlyicp:
        mode_name = "only_icp"
        chosen_mode_function = lambda db_conn: run_only_icp_mode(db_conn, args)
    elif args.onlyquake:
        mode_name = "only_quake"
        chosen_mode_function = lambda db_conn: run_only_quake_mode(db_conn, args)
    elif args.basic:
        mode_name = "basic"
        chosen_mode_function = lambda db_conn: run_basic_mode(db_conn, args)
    else:  # advanced is the default
        mode_name = "advanced"
        chosen_mode_function = lambda db_conn: run_advanced_mode(db_conn, args)

    LOG_FILE_PATH = f"log_icp_{mode_name}.txt"
    # --- 核心修改：现在OUTPUT_BASE_DIR总是从-o参数获取 ---
    OUTPUT_BASE_DIR = args.output

    configure_logging(LOG_FILE_PATH)
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    script_start_time = time.time()
    db_conn = initialize_database()
    if not db_conn:
        cs_console.print("[bold red]CRITICAL:[/bold red] 无法连接到数据库，脚本将退出。")
        return

    cs_console.print(
        f"[bold underline green]启动 {mode_name.replace('_', '-').capitalize()} 模式[/bold underline green]")
    chosen_mode_function(db_conn)

    if db_conn: db_conn.close()

    overall_duration = time.time() - script_start_time
    cs_console.print(
        f"\n[bold green]脚本整体执行完毕于: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold green]")
    cs_console.print(f"脚本整体运行时间: {overall_duration:.2f} 秒 ({datetime.timedelta(seconds=overall_duration)}).")


if __name__ == "__main__":
    main()
