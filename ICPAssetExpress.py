import argparse
import base64
import datetime
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

import pandas as pd
import requests
from rich.console import Console

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
BATCH_SIZE = 1000
DELAY = 3
DEFAULT_PORTS = {
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
QUAKE_QUERY_TEMPLATE = 'icp_keywords:"{target}" and country:"China" AND not province:"Hongkong"'
# QUAKE_QUERY_TEMPLATE = 'icp_keywords:"{target}" and not domain_is_wildcard:true and country:"China" AND not province:"Hongkong"'
SHOW_SCAN_INFO = False

# --- Fofa配置 ---
FOFA_EMAIL = ""
FOFA_KEY = ""
FOFA_BASE_URL = "https://fofa.info"

# --- wer.plus API配置 ---
WERPLUS_API_KEY = ""


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
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS Targets (target_id INTEGER PRIMARY KEY AUTOINCREMENT, target_name TEXT UNIQUE NOT NULL, last_queried_quake TIMESTAMP, last_queried_fofa TIMESTAMP, notes TEXT);")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS QuakeRawData (data_id INTEGER PRIMARY KEY AUTOINCREMENT, target_id INTEGER NOT NULL, query_timestamp TIMESTAMP NOT NULL, raw_json TEXT NOT NULL, FOREIGN KEY (target_id) REFERENCES Targets (target_id));")
        try:
            cursor.execute("ALTER TABLE Targets ADD COLUMN last_queried_fofa TIMESTAMP;")
        except sqlite3.OperationalError:
            pass
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS FofaRuns (fofa_run_id INTEGER PRIMARY KEY AUTOINCREMENT, target_id INTEGER NOT NULL, run_timestamp TIMESTAMP NOT NULL, status TEXT DEFAULT 'pending', input_ip_count INTEGER DEFAULT 0, found_results_count INTEGER DEFAULT 0, notes TEXT, FOREIGN KEY (target_id) REFERENCES Targets (target_id));")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS FofaRawData (fofa_data_id INTEGER PRIMARY KEY AUTOINCREMENT, fofa_run_id INTEGER NOT NULL, raw_json TEXT NOT NULL, FOREIGN KEY (fofa_run_id) REFERENCES FofaRuns (fofa_run_id));")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS CompanyAppCache (cache_id INTEGER PRIMARY KEY AUTOINCREMENT, company_name TEXT UNIQUE NOT NULL, last_queried TIMESTAMP NOT NULL, raw_json_apps TEXT, raw_json_miniprograms TEXT);")
        conn.commit()
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
    return re.sub(f'[{re.escape(invalid_chars)}]', '', name_str)[:31]


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

        raw_json_list = [json.loads(row[0]) for row in cached_rows]
        parsed_data = parse_results(raw_json_list)
        cs_console.print(
            f"    [green]缓存命中:[/green] '{target_name}' 从数据库加载并解析 {len(parsed_data)} 条Quake记录。")
        return parsed_data
    except (sqlite3.Error, json.JSONDecodeError, ValueError) as e:
        logging.error(f"检查Quake缓存时出错 ({target_name}): {e}", exc_info=True)
        return None


def query_all_pages(target_name, db_conn):
    cs_console.print(f"    [blue]API查询:[/blue] 目标 '{target_name}'，开始通过Quake API获取数据...")
    headers = {"X-QuakeToken": API_KEY, "Content-Type": "application/json"}
    query_dsl = QUAKE_QUERY_TEMPLATE.format(target=target_name)
    all_raw_data = []
    pagination_id = None

    try:
        while True:
            params = {"query": query_dsl, "size": BATCH_SIZE, "ignore_cache": False, "latest": True}
            if pagination_id:
                params["pagination_id"] = pagination_id

            response = requests.post(f"{BASE_URL}/scroll/quake_service", headers=headers, json=params, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != 0:
                cs_console.print(f"[bold red]Error:[/bold red] Quake API 查询失败: {result.get('message')}")
                return None  # API返回明确错误，直接返回None

            current_batch = result.get("data", [])

            # --- 最终的、最可靠的终止条件 ---
            # 如果API返回的数据批次为空，说明已经取完所有数据，这是唯一需要依赖的判断。
            if not current_batch:
                break  # 正常取完所有数据，退出循环

            # 只有在有数据的情况下，才处理数据和更新翻页ID
            all_raw_data.extend(current_batch)
            pagination_id = result.get("meta", {}).get("pagination_id")

            # 如果Quake在有数据的情况下不返回下一个翻页ID，也视为结束（保险措施）
            if not pagination_id:
                break

            time.sleep(DELAY)

        # 循环结束后的处理逻辑
        cs_console.print(f"    [green]API查询成功:[/green] 共获取 {len(all_raw_data)} 条原始记录。")

        # --- 后续数据库操作 ---
        cursor = db_conn.cursor()
        timestamp = datetime.datetime.now()
        target_id = get_target_id_from_db(target_name, db_conn)
        if not target_id: return None

        cursor.execute("DELETE FROM QuakeRawData WHERE target_id = ?", (target_id,))
        if all_raw_data:
            data_to_insert = [(target_id, timestamp, json.dumps(item, ensure_ascii=False)) for item in all_raw_data]
            cursor.executemany("INSERT INTO QuakeRawData (target_id, query_timestamp, raw_json) VALUES (?, ?, ?)",
                               data_to_insert)

        cursor.execute("UPDATE Targets SET last_queried_quake = ? WHERE target_id = ?", (timestamp, target_id))
        db_conn.commit()
        return parse_results(all_raw_data)

    except requests.exceptions.RequestException as e:
        cs_console.print(f"[bold red]Error:[/bold red] Quake API 请求异常: {e}")
        return None
    except Exception as e:
        logging.error(f"Quake API处理中发生未知异常 ({target_name}): {e}", exc_info=True)
        return None


def parse_results(raw_data_list_objs):
    """
    (双协议URL优化版) 解析Quake数据。
    - 手动拼接URL时，会同时生成http和https两个版本。
    - 在内部用 scan_urls 键存储所有待扫描的URL，保持主URL字段整洁。
    """
    parsed_results = []
    for raw_data_obj in raw_data_list_objs:
        service_info = raw_data_obj.get("service", {})
        http_info = service_info.get("http", {})
        icp_info = http_info.get("icp", {})
        main_icp = icp_info.get("main_licence", {}) if icp_info else {}
        location_info = raw_data_obj.get("location", {})
        port = raw_data_obj.get("port")

        url_for_display = ""
        scan_urls = set()  # 用于存储所有需要扫描的URL

        # 优先级1 & 2: 使用Quake直接提供的URL
        direct_url = (http_info.get("http_load_url") or [http_info.get("url")])[0]
        if direct_url:
            url_for_display = direct_url
            scan_urls.add(direct_url)
        # 优先级3: 智能手动拼接 (双协议)
        else:
            host = http_info.get("host") or raw_data_obj.get("domain") or raw_data_obj.get("ip")
            if host:
                # 拼接HTTP版本
                http_url = f"http://{host}" + (f":{port}" if port != 80 else "")
                scan_urls.add(http_url)

                # 拼接HTTPS版本
                https_url = f"https://{host}" + (f":{port}" if port != 443 else "")
                scan_urls.add(https_url)

                # 在报告中默认显示HTTP版本作为主URL
                url_for_display = http_url

        # 产品指纹提取逻辑 (保持不变)
        components = raw_data_obj.get("components", [])
        fingerprints = []
        if components:
            for component in components:
                name = component.get("product_name_cn") or component.get("product_name_en")
                version = component.get("version", "")
                if name:
                    fingerprints.append(f"{name} ({version})" if version else name)
        fingerprints_str = "\n".join(fingerprints)

        clean_title = (http_info.get("title", "") or "").replace("\n", " ").replace("\r", " ").strip()

        parsed = {
            "IP": raw_data_obj.get("ip", ""), "Port": str(port or ""), "Host": http_info.get("host", ""),
            "URL": url_for_display,  # Excel中只显示一个主URL
            "HTTP状态码": http_info.get("status_code", ""),
            "scan_urls": list(scan_urls),  # 新增：内部使用的待扫描URL列表
            "Domain": raw_data_obj.get("domain", ""),
            "网站标题": clean_title,
            "产品指纹": fingerprints_str,
            "备案号": icp_info.get("licence", "") if icp_info else "",
            "主体单位": main_icp.get("unit", ""),
            "备案单位类型": main_icp.get("nature", ""), "时间": raw_data_obj.get("time", ""),
            "归属省份": location_info.get("province_cn", "")
        }
        parsed_results.append(parsed)
    return parsed_results


def identify_shared_service_ips(raw_quake_data_list):
    shared_service_ips = set()
    cs_console.print("    [blue]执行:[/blue] 智能过滤IP，以优化Fofa反查目标...")
    all_ips = {item.get("IP") for item in raw_quake_data_list if item.get("IP")}
    SAN_THRESHOLD = 20
    PUBLIC_SERVICE_KEYWORDS = [
        'qiye.aliyun.com', 'exmail.qq.com', 'qiye.163.com', 'ali-mail.com', 'dingtalk.com', 'mxhichina.com',
        '.cdn.cloudflare.net', '.akamaiedge.net', '.fastly.net', '.chinacache.com', '.cdnetworks.net', 'aliyuncs.com',
        'bspapp.com', 'hiflow.tencent.com'
    ]
    for raw_data in raw_quake_data_list:
        ip = raw_data.get("IP")
        if not ip: continue
        try:
            if len(raw_data['service']['tls']['certificate']['parsed']['extensions']['subject_alt_name'][
                       'dns_names']) > SAN_THRESHOLD:
                shared_service_ips.add(ip);
                continue
        except (KeyError, TypeError):
            pass
        try:
            for component in raw_data.get('components', []):
                if "内容分发网络(CDN)" in component.get('product_type', []) or '企业邮箱' in component.get(
                        'product_name_cn', ''):
                    shared_service_ips.add(ip);
                    break
        except (KeyError, TypeError):
            pass
        if ip in shared_service_ips: continue
        try:
            for record in raw_data.get('service', {}).get('dns', {}).get('cname', []):
                if any(keyword in record.lower() for keyword in PUBLIC_SERVICE_KEYWORDS):
                    shared_service_ips.add(ip);
                    break
        except (KeyError, TypeError):
            pass
        if ip in shared_service_ips: continue
        try:
            if 'Aliyun URL Forwarding Server' in raw_data.get('service', {}).get('http', {}).get('response_headers',
                                                                                                 ''):
                shared_service_ips.add(ip)
        except (KeyError, TypeError):
            pass
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
    if not ip_list: return [], None
    fofa_run_id = None
    try:
        cursor = db_conn.cursor()
        cursor.execute("INSERT INTO FofaRuns (target_id, run_timestamp, status, input_ip_count) VALUES (?, ?, ?, ?)",
                       (target_id, datetime.datetime.now(), 'running', len(ip_list)))
        db_conn.commit()
        fofa_run_id = cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Fofa: 创建FofaRuns记录失败: {e}", exc_info=True)
        return [], None
    all_fofa_raw_results = []
    api_call_overall_successful = True
    ip_chunks = [ip_list[i:i + 100] for i in range(0, len(ip_list), 100)]
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
                api_url = f"{FOFA_BASE_URL}/api/v1/search/next?email={FOFA_EMAIL}&key={FOFA_KEY}&qbase64={qbase64}&fields={fields}&size=2000"
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


def _fetch_icpb_data(company_name, app_type):
    api_url = "https://api2.wer.plus/api/icpb"
    all_results, page = [], 1
    while True:
        params = {'key': WERPLUS_API_KEY, 't': company_name, 'page': page, 'pagesize': 40, 'apptype': app_type}
        try:
            response = requests.get(api_url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 200 and data.get("data"):
                current_page_results = data["data"].get("list", [])
                if not current_page_results: break
                all_results.extend(current_page_results)
                if len(all_results) >= data['data'].get('total', 0): break
                page += 1
            else:
                break
        except requests.exceptions.RequestException:
            break
    return all_results


def check_and_get_app_cache(company_name, db_conn):
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT last_queried, raw_json_apps, raw_json_miniprograms FROM CompanyAppCache WHERE company_name = ?",
            (company_name,))
        row = cursor.fetchone()
        if not row: return None
        last_queried_dt = datetime.datetime.fromisoformat(row[0])
        if (datetime.datetime.now() - last_queried_dt).total_seconds() / 3600 >= CACHE_EXPIRY_HOURS: return None
        return {"apps": json.loads(row[1] or '[]'), "miniprograms": json.loads(row[2] or '[]')}
    except Exception:
        return None


def query_apps_and_miniprograms(company_name, db_conn, types_to_check):
    cached_data = check_and_get_app_cache(company_name, db_conn)
    if cached_data:
        app_results = cached_data.get("apps", []) if 'app' in types_to_check else []
        miniprogram_results = cached_data.get("miniprograms", []) if 'mapp' in types_to_check else []
    else:
        live_apps = _fetch_icpb_data(company_name, 'app')
        live_miniprograms = _fetch_icpb_data(company_name, 'mapp')
        try:
            cursor = db_conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO CompanyAppCache (company_name, last_queried, raw_json_apps, raw_json_miniprograms) VALUES (?, ?, ?, ?)",
                (company_name, datetime.datetime.now(), json.dumps(live_apps), json.dumps(live_miniprograms)))
            db_conn.commit()
        except sqlite3.Error as e:
            logging.error(f"写入APP缓存失败: {e}")
        app_results = live_apps if 'app' in types_to_check else []
        miniprogram_results = live_miniprograms if 'mapp' in types_to_check else []
    combined = [{'detected_type': 'app', **item} for item in app_results] + [{'detected_type': 'mapp', **item} for item
                                                                             in miniprogram_results]
    return combined


def parse_app_results(raw_data_list):
    parsed_list = []
    for item in raw_data_list:
        parsed_item = {'备案主体名': item.get('unitName', ''), '小程序': '', 'app': '',
                       '备案号': item.get('serviceLicence', ''), '单位性质': item.get('natureName', ''),
                       '审核时间': item.get('updateRecordTime', '')}
        if item.get('detected_type') == 'mapp':
            parsed_item['小程序'] = item.get('serviceName', '')
        elif item.get('detected_type') == 'app':
            parsed_item['app'] = item.get('serviceName', '')
        parsed_list.append(parsed_item)
    return parsed_list


# ======================= 文件输出与处理 =======================
def write_quake_results_to_excel(output_dir, company_name, data, stage=""):
    """(最终格式化版) 保存Quake结果，移除指定列，并防止URL自动超链接。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    excel_path = os.path.join(output_dir, f"quake_result{filename_suffix}.xlsx")

    if not data:
        return

    # --- 核心修改 1: 在创建DataFrame之前，先从每条数据中移除不需要的列 ---
    columns_to_remove = ['scan_urls', 'Host']
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


def write_app_results_to_excel(output_dir, company_name, data):
    if not data: return
    filename_suffix = generate_filename_suffix(company_name, "apps")
    excel_path = os.path.join(output_dir, f"app_results{filename_suffix}.xlsx")
    try:
        pd.DataFrame(data).to_excel(excel_path, index=False, sheet_name="App_MiniProgram_Data")
        cs_console.print(
            f"      [green]Success:[/green] 小程序/APP Excel已保存: '{os.path.basename(excel_path)}' ({len(data)} 条)")
    except Exception as e:
        logging.error(f"保存小程序/APP Excel失败: {e}")


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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_dir = os.path.join(script_dir, 'tools')
    observer_ward_path = os.path.join(tools_dir, 'observer_ward.exe')
    if not os.path.exists(observer_ward_path):
        cs_console.print(f"  [bold red]Error:[/bold red] observer_ward.exe 未找到，跳过指纹识别。")
        return
    input_file = write_urls_to_txt_file(company_dir_path, company_name, urls_to_fingerprint, f"observer_input_{stage}")
    if not input_file: return
    output_file = os.path.join(company_dir_path, f"url_fingerprint{generate_filename_suffix(company_name, stage)}.csv")
    command = [observer_ward_path, '-l', input_file, '-o', output_file]
    if not SHOW_SCAN_INFO: command.append('--silent')
    cs_console.print(f"    [blue]执行:[/blue] observer_ward URL指纹识别 ({stage})...")
    try:
        subprocess.run(command, check=True, cwd=tools_dir, capture_output=not SHOW_SCAN_INFO, text=True,
                       encoding='utf-8', errors='ignore')
        cs_console.print(f"      [green]Success:[/green] 指纹识别结果已保存: '{os.path.basename(output_file)}'")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logging.error(f"observer_ward 执行失败: {e}", exc_info=True)
        cs_console.print(f"      [bold red]Error:[/bold red] observer_ward 执行失败 (详情见日志)。")


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
def run_only_quake_mode(db_conn, skip_fofa_fingerprint=False, no_fofa=False, types_to_check=None):
    """
    (最终格式化版) 仅查询Quake资产，合并输出到格式精美的Excel总表，并移除指定列。
    """
    start_time_quake_only = time.time()
    cs_console.print(f"[bold blue]Quake-Only 模式启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    failed_targets = []
    all_quake_assets = []  # 用于存储所有目标的所有资产

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

        # 1. 数据获取 (Quake only)
        parsed_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_quake_data is None:
            cs_console.print(f"    [blue]INFO:[/blue] 无有效缓存，执行实时API查询...")
            parsed_quake_data = query_all_pages(target_name, db_conn)

        if parsed_quake_data is None:
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败或出错'})
            continue
        if not parsed_quake_data:
            failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'})
            cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 无Quake资产。")
            continue

        # 2. 将获取到的数据直接添加到总列表中
        cs_console.print(f"  [green]数据处理完成:[/green] 发现 {len(parsed_quake_data)} 条资产记录。")
        all_quake_assets.extend(parsed_quake_data)

    # 3. 在所有目标处理完毕后，统一写入一个总文件
    if all_quake_assets:
        cs_console.print(f"\n[bold blue]开始生成Quake资产总报告...[/bold blue]")
        output_path = os.path.join(OUTPUT_BASE_DIR, "Quake_Only_Results_all.xlsx")
        try:
            # --- 核心修改部分 ---
            # 1. 创建初始DataFrame并移除指定列 (满足上一个需求)
            df = pd.DataFrame(all_quake_assets)
            columns_to_remove = ['Host', 'scan_urls']
            df.drop(columns=columns_to_remove, inplace=True, errors='ignore')

            # 2. 使用xlsxwriter引擎来写入Excel，以便添加自定义格式 (满足当前需求)
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name="Quake_Data_Summary", index=False)

                # 获取工作簿和工作表对象
                workbook = writer.book
                worksheet = writer.sheets["Quake_Data_Summary"]

                # --- 定义格式 ---
                # 自动换行 + 顶部对齐 的格式
                wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                # 纯文本格式 (用于URL列，防止自动生成超链接) + 顶部对齐
                text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})
                # 默认的顶部对齐格式
                top_align_format = workbook.add_format({'valign': 'top'})

                # 定义需要自动换行的列，除了"产品指纹"，"网站标题"也可能需要
                wrap_columns = ['产品指纹', '网站标题']

                # --- 应用格式 ---
                # (A) 设置列宽和单元格格式
                for col_num, col_name in enumerate(df.columns):
                    # 根据内容计算一个合适的列宽
                    try:
                        max_len = max(df[col_name].astype(str).map(len).max(), len(col_name))
                    except (ValueError, TypeError):
                        max_len = len(col_name)
                    width = min(max_len + 5, 60)  # 宽度上限设为60，防止过宽

                    # 根据列名应用不同的格式
                    if col_name == 'URL':
                        worksheet.set_column(col_num, col_num, width, text_format)
                    elif col_name in wrap_columns:
                        worksheet.set_column(col_num, col_num, width, wrap_format)
                    else:
                        worksheet.set_column(col_num, col_num, width, top_align_format)

                # (B) 设置行高
                for row_num in range(len(df)):
                    max_lines = 1
                    # 检查需要换行的列，计算哪一列的行数最多
                    for col_name in wrap_columns:
                        if col_name in df.columns:
                            cell_value = str(df.iloc[row_num][col_name])
                            # 通过计算换行符'\n'的数量来估算行数
                            num_lines = cell_value.count('\n') + 1
                            max_lines = max(max_lines, num_lines)

                    # 如果内容超过一行，则根据最大行数设置一个合适的行高
                    if max_lines > 1:
                        # 15是经验值，大致为一行的磅值高度
                        worksheet.set_row(row_num + 1, max_lines * 15)
                    else:
                        worksheet.set_row(row_num + 1, None, top_align_format)

            cs_console.print(
                f"  [green]Success:[/green] Quake资产总报告已保存到: '{output_path}' (共 {len(df)} 条)")
        except Exception as e:
            cs_console.print(f"  [bold red]Error:[/bold red] 写入总报告失败: {e}")
            logging.error(f"写入Quake总报告失败: {e}", exc_info=True)
    else:
        cs_console.print("\n[yellow]INFO:[/yellow] 未发现任何Quake资产，不生成总报告。")

    # 仍然可以生成一个关于查询失败目标的自查报告
    create_self_check_report(failed_targets, db_conn, "only_quake")

    cs_console.print(
        f"\n[bold green]Quake-Only 模式结束.[/bold green] 总耗时: {round(time.time() - start_time_quake_only, 2)} 秒.")


def run_basic_mode(db_conn, skip_fofa_fingerprint=False, no_fofa=False, types_to_check=None):
    start_time_basic = time.time()
    cs_console.print(f"[bold blue]基础模式启动...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return

    failed_targets, grand_total_apps_list = [], []

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

        parsed_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_quake_data is None:
            cs_console.print(f"    [blue]INFO:[/blue] 无有效缓存，执行实时API查询...")
            parsed_quake_data = query_all_pages(target_name, db_conn)

        if parsed_quake_data is None:
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败或出错'});
            continue
        if not parsed_quake_data:
            failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'})
            cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 无Quake资产，跳过后续处理。");
            continue

        target_id = get_target_id_from_db(target_name, db_conn)
        target_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(target_name))
        os.makedirs(target_dir, exist_ok=True)

        assets_from_quake = defaultdict(lambda: {"ips": set(), "urls": set(), "raw_data": []})
        for item in parsed_quake_data:
            company_key = item.get("主体单位") or "未知主体单位_Basic"
            assets_from_quake[company_key]["raw_data"].append(item)
            if item.get("IP"): assets_from_quake[company_key]["ips"].add(item.get("IP"))

            # --- 核心修改：与高级模式同步，从新的 scan_urls 字段聚合URL ---
            if item.get("scan_urls"):
                assets_from_quake[company_key]["urls"].update(item.get("scan_urls"))

        total_companies = len(assets_from_quake)
        cs_console.print(f"  [green]Quake数据处理完成:[/green] 发现 {total_companies} 个主体单位。")

        for company_index, (company_name, assets) in enumerate(assets_from_quake.items(), 1):
            cs_console.print(f"\n  ({company_index}/{total_companies}) 处理主体单位: [cyan]{company_name}[/cyan]")
            company_dir = os.path.join(target_dir, sanitize_sheet_name(company_name))
            os.makedirs(company_dir, exist_ok=True)

            write_quake_results_to_excel(company_dir, company_name, assets["raw_data"], stage="quake")

            http_urls_from_quake = [url for url in assets["urls"] if
                                    url and url.lower().startswith(('http://', 'https://'))]
            if http_urls_from_quake:
                run_observer_ward(company_name, company_dir, http_urls_from_quake, stage="fingerprint_from_quake")

            if types_to_check and "未知主体" not in company_name:
                cs_console.print(f"\n    [blue]执行:[/blue] 开始查询 '{company_name}' 相关的APP/小程序信息...")
                raw_app_data = query_apps_and_miniprograms(company_name, db_conn, types_to_check)
                if raw_app_data:
                    parsed_app_data = parse_app_results(raw_app_data)
                    write_app_results_to_excel(company_dir, company_name, parsed_app_data)
                    grand_total_apps_list.extend(parsed_app_data)
                else:
                    cs_console.print(f"      [yellow]INFO:[/yellow] 未找到 '{company_name}' 相关的APP或小程序信息。")

            archive_intermediate_files(company_dir, company_name)

        if not no_fofa and target_id:
            cs_console.print(f"\n[bold blue]>>>>>> 开始对目标 '{target_name}' 进行Fofa IP反查 <<<<<<[/bold blue]")
            all_ips = {ip for assets in assets_from_quake.values() for ip in assets["ips"] if ip}
            if all_ips:
                fofa_target_ips, filtered_out_ips = identify_shared_service_ips(parsed_quake_data)
                fofa_output_dir = os.path.join(target_dir, "fofa_results")
                os.makedirs(fofa_output_dir, exist_ok=True)
                if filtered_out_ips:
                    filtered_ip_file = write_ips_to_file(fofa_output_dir, target_name, filtered_out_ips,
                                                         "filtered_ips_for_fofa")
                    if filtered_ip_file:
                        cs_console.print(
                            f"      - [dim]被过滤的共享IP已保存到 '{os.path.basename(filtered_ip_file)}'[/dim]")
                if fofa_target_ips:
                    cs_console.print(
                        f"    [blue]执行:[/blue] 将对过滤后的 {len(fofa_target_ips)} 个独立IP进行Fofa反查。")
                    fofa_parsed_data = check_and_get_fofa_cache(target_id, db_conn)
                    if fofa_parsed_data is None:
                        fofa_raw_data, _ = query_fofa_by_ips(fofa_target_ips, target_id, db_conn)
                        fofa_parsed_data = parse_fofa_results(fofa_raw_data) if fofa_raw_data else []
                    if fofa_parsed_data:
                        write_fofa_results_to_excel(fofa_output_dir, target_name, fofa_parsed_data)
                        if not skip_fofa_fingerprint:
                            fofa_urls = [item["URL"] for item in fofa_parsed_data if
                                         item.get("URL", "").lower().startswith(('http://', 'https://'))]
                            if fofa_urls: run_observer_ward(target_name, fofa_output_dir, fofa_urls,
                                                            stage="fingerprint_from_fofa")
                else:
                    cs_console.print("      [yellow]INFO:[/yellow] 过滤后无独立IP可用于Fofa反查。")
            else:
                cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 未发现任何IP，跳过Fofa反查。")

    if grand_total_apps_list:
        write_final_summary_report(OUTPUT_BASE_DIR, grand_total_apps_list)
    process_all_generated_csvs(OUTPUT_BASE_DIR)
    create_self_check_report(failed_targets, db_conn, "basic")
    end_time_basic = time.time()
    cs_console.print(
        f"\n[bold green]基础模式结束.[/bold green] 总耗时: {round(end_time_basic - start_time_basic, 2)} 秒.")


def run_advanced_mode(db_conn, skip_fofa_fingerprint=False, no_fofa=False, types_to_check=None):
    start_time_advanced = time.time()
    cs_console.print(f"[bold blue]高级模式启动 (Gogo集成)...[/bold blue]")
    target_names = load_queries(INPUT_FILE)
    if not target_names: return
    failed_targets, grand_total_apps_list = [], []

    for index, target_name in enumerate(target_names, 1):
        cs_console.print(
            f"\n[bold magenta]>>>>>> 开始处理目标 ({index}/{len(target_names)}): '{target_name}' <<<<<<[/bold magenta]")

        parsed_quake_data = check_and_get_quake_cache(target_name, db_conn)
        if parsed_quake_data is None:
            cs_console.print(f"    [blue]INFO:[/blue] 无有效缓存，执行实时API查询...")
            parsed_quake_data = query_all_pages(target_name, db_conn)

        if parsed_quake_data is None:
            failed_targets.append({'name': target_name, 'reason': 'API查询过程失败或出错'});
            continue
        if not parsed_quake_data:
            failed_targets.append({'name': target_name, 'reason': '查询成功但无结果'})
            cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 无Quake资产，跳过后续处理。");
            continue

        target_id = get_target_id_from_db(target_name, db_conn)
        target_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(target_name))
        os.makedirs(target_dir, exist_ok=True)

        assets_from_quake = defaultdict(lambda: {"ips": set(), "urls": set(), "allPort": set(), "raw_data": []})
        for item in parsed_quake_data:
            company_key = item.get("主体单位") or "未知主体单位_Advanced"
            assets_from_quake[company_key]["raw_data"].append(item)
            if item.get("IP"): assets_from_quake[company_key]["ips"].add(item.get("IP"))

            # --- 核心修改：从新的 scan_urls 字段聚合所有待扫描URL ---
            if item.get("scan_urls"):
                assets_from_quake[company_key]["urls"].update(item.get("scan_urls"))

            if item.get("Port"): assets_from_quake[company_key]["allPort"].add(str(item.get("Port")))

        total_companies = len(assets_from_quake)
        cs_console.print(f"  [green]Quake数据处理完成:[/green] 发现 {total_companies} 个主体单位。")

        for company_index, (company_name, assets) in enumerate(assets_from_quake.items(), 1):
            cs_console.print(f"\n  ({company_index}/{total_companies}) 处理主体单位: [cyan]{company_name}[/cyan]")
            company_dir = os.path.join(target_dir, sanitize_sheet_name(company_name))
            os.makedirs(company_dir, exist_ok=True)

            write_quake_results_to_excel(company_dir, company_name, assets["raw_data"], stage="quake")

            http_urls_from_quake = [url for url in assets["urls"] if
                                    url and url.lower().startswith(('http://', 'https://'))]
            if http_urls_from_quake:
                run_observer_ward(company_name, company_dir, http_urls_from_quake, stage="fingerprint_from_quake")

            # ... 后续 Gogo, Fofa, APP 查询逻辑保持不变 ...
            company_ips_list = list(assets["ips"])
            if company_ips_list:
                ip_list_file = write_ips_to_file(company_dir, company_name, company_ips_list, "gogo_input")
                if ip_list_file:
                    ports_to_scan = assets["allPort"] | DEFAULT_PORTS
                    cs_console.print(f"\n    [blue]Gogo主动扫描准备:[/blue]")
                    cs_console.print(
                        f"      - [dim]将对 {len(company_ips_list)} 个IP的 {len(ports_to_scan)} 个端口进行扫描。[/dim]")
                    gogo_output_path = run_gogo_scan(company_name, ip_list_file, list(ports_to_scan), company_dir)
                    if gogo_output_path:
                        new_urls_from_gogo = process_gogo_output_and_generate_excel(gogo_output_path, company_name,
                                                                                    company_dir)
                        if new_urls_from_gogo:
                            run_observer_ward(company_name, company_dir, new_urls_from_gogo, "fingerprint_from_gogo")

            if types_to_check and "未知主体" not in company_name:
                cs_console.print(f"\n    [blue]执行:[/blue] 开始查询 '{company_name}' 相关的APP/小程序信息...")
                raw_app_data = query_apps_and_miniprograms(company_name, db_conn, types_to_check)
                if raw_app_data:
                    parsed_app_data = parse_app_results(raw_app_data)
                    write_app_results_to_excel(company_dir, company_name, parsed_app_data)
                    grand_total_apps_list.extend(parsed_app_data)
                else:
                    cs_console.print(f"      [yellow]INFO:[/yellow] 未找到 '{company_name}' 相关的APP或小程序信息。")

            archive_intermediate_files(company_dir, company_name)

        if not no_fofa and target_id:
            cs_console.print(f"\n[bold blue]>>>>>> 开始对目标 '{target_name}' 进行Fofa IP反查 <<<<<<[/bold blue]")
            all_ips = {ip for assets in assets_from_quake.values() for ip in assets["ips"] if ip}
            raw_quake_for_filter = [item for assets in assets_from_quake.values() for item in assets["raw_data"]]
            if all_ips:
                fofa_target_ips, filtered_out_ips = identify_shared_service_ips(raw_quake_for_filter)
                fofa_output_dir = os.path.join(target_dir, "fofa_results")
                os.makedirs(fofa_output_dir, exist_ok=True)
                if filtered_out_ips:
                    filtered_ip_file = write_ips_to_file(fofa_output_dir, target_name, filtered_out_ips,
                                                         "filtered_ips_for_fofa")
                    if filtered_ip_file:
                        cs_console.print(
                            f"      - [dim]被过滤的共享IP已保存到 '{os.path.basename(filtered_ip_file)}'[/dim]")
                if fofa_target_ips:
                    cs_console.print(
                        f"    [blue]执行:[/blue] 将对过滤后的 {len(fofa_target_ips)} 个独立IP进行Fofa反查。")
                    fofa_parsed_data = check_and_get_fofa_cache(target_id, db_conn)
                    if fofa_parsed_data is None:
                        fofa_raw_data, _ = query_fofa_by_ips(fofa_target_ips, target_id, db_conn)
                        fofa_parsed_data = parse_fofa_results(fofa_raw_data) if fofa_raw_data else []
                    if fofa_parsed_data:
                        write_fofa_results_to_excel(fofa_output_dir, target_name, fofa_parsed_data)
                        if not skip_fofa_fingerprint:
                            fofa_urls = [item["URL"] for item in fofa_parsed_data if
                                         item.get("URL", "").lower().startswith(('http://', 'https://'))]
                            if fofa_urls: run_observer_ward(target_name, fofa_output_dir, fofa_urls,
                                                            stage="fingerprint_from_fofa")
                else:
                    cs_console.print("      [yellow]INFO:[/yellow] 过滤后无独立IP可用于Fofa反查。")
            else:
                cs_console.print(f"    [yellow]INFO:[/yellow] 目标 '{target_name}' 未发现任何IP，跳过Fofa反查。")

    if grand_total_apps_list:
        write_final_summary_report(OUTPUT_BASE_DIR, grand_total_apps_list)
    process_all_generated_csvs(OUTPUT_BASE_DIR)
    create_self_check_report(failed_targets, db_conn, "advanced_gogo")
    cs_console.print(
        f"\n[bold green]高级模式结束.[/bold green] 总耗时: {round(time.time() - start_time_advanced, 2)} 秒.")


def main():
    global SHOW_SCAN_INFO, INPUT_FILE, API_KEY, OUTPUT_BASE_DIR, FOFA_EMAIL, FOFA_KEY, WERPLUS_API_KEY

    parser = argparse.ArgumentParser(
        description="ICP Asset Express - Gogo 集成版: 自动化ICP备案资产梳理与安全评估工具。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=f"""
    使用示例:
      python {os.path.basename(__file__)} --onlyquake -i my_targets.txt
      python {os.path.basename(__file__)} -a -i my_targets.txt -o ./my_scan_results
      python {os.path.basename(__file__)} -b --apikey YOUR_KEY -checkother app,mapp
    """
    )
    # --- 核心修改 1: 将新参数加入互斥组 ---
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--onlyquake', action='store_true', help="仅查询Quake资产并输出表格，不进行任何主动扫描")
    mode_group.add_argument('-b', '--basic', action='store_true', help="运行基础模式")
    mode_group.add_argument('-a', '--advanced', action='store_true', help="运行高级模式 (使用gogo进行扫描, 默认)")

    parser.add_argument('-i', '--input', type=str, help=f"指定输入文件名。默认为: '{INPUT_FILE}'。")
    parser.add_argument('-o', '--output', type=str, help="指定自定义的输出根目录。")
    parser.add_argument('--apikey', type=str, help="指定360 Quake API Key。")
    parser.add_argument('--fofa-email', type=str, help="Fofa注册邮箱")
    parser.add_argument('--fofa-key', type=str, help="Fofa API Key")
    parser.add_argument('--werplus-key', type=str, help="wer.plus API Key")
    parser.add_argument('--showScanInfo', action='store_true', help="显示外部扫描工具的实时运行输出。")
    parser.add_argument('--skip-fofa-fingerprint', action='store_true', help="跳过对Fofa反查结果的URL进行指纹识别。")
    parser.add_argument('--no-fofa', action='store_true', help="完全跳过Fofa IP反查流程。")
    parser.add_argument('-checkother', type=str, help="查询额外信息，多个用逗号分隔 (app,mapp)。")
    args = parser.parse_args()

    # --- 核心修改 2: 调整模式选择逻辑 ---
    if not args.onlyquake and not args.basic and not args.advanced:
        args.advanced = True  # 如果不指定任何模式，默认为高级模式

    SHOW_SCAN_INFO = args.showScanInfo
    if args.input: INPUT_FILE = args.input
    if args.apikey: API_KEY = args.apikey
    if args.fofa_email: FOFA_EMAIL = args.fofa_email
    if args.fofa_key: FOFA_KEY = args.fofa_key
    if args.werplus_key: WERPLUS_API_KEY = args.werplus_key
    types_to_check = [t.strip().lower() for t in args.checkother.split(',')] if args.checkother else []

    # 根据模式设置函数、日志和输出目录
    if args.onlyquake:
        mode_name = "only_quake"
        chosen_mode_function = run_only_quake_mode
    elif args.basic:
        mode_name = "basic"
        chosen_mode_function = run_basic_mode
    else:  # Default to advanced
        mode_name = "advanced"
        chosen_mode_function = run_advanced_mode

    LOG_FILE_PATH = f"log_icp_{mode_name}.txt"
    OUTPUT_BASE_DIR = args.output if args.output else f"results_icp_{mode_name}"

    configure_logging(LOG_FILE_PATH)
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    script_start_time = time.time()
    db_conn = initialize_database()
    if not db_conn:
        cs_console.print("[bold red]CRITICAL:[/bold red] 无法连接到数据库，脚本将退出。")
        return

    cs_console.print(
        f"[bold underline green]启动 {mode_name.replace('_', '-').capitalize()} 模式[/bold underline green]")
    # 调用选定的主函数
    chosen_mode_function(db_conn, args.skip_fofa_fingerprint, args.no_fofa, types_to_check)

    if db_conn: db_conn.close()

    overall_duration = time.time() - script_start_time
    cs_console.print(
        f"\n[bold green]脚本整体执行完毕于: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold green]")
    cs_console.print(f"脚本整体运行时间: {overall_duration:.2f} 秒 ({datetime.timedelta(seconds=overall_duration)}).")


if __name__ == "__main__":
    main()
