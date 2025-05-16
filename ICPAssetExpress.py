import os
import os.path
import re
import subprocess
import tempfile
import time
import datetime
import logging
import argparse  # 用于处理命令行参数
import math  # 用于对数计算

import pandas as pd
import requests
from rich import box
from rich.console import Console
from rich.table import Table
from collections import defaultdict

# --- Global Configuration (全局配置，部分可被命令行参数覆盖) ---
OUTPUT_BASE_DIR = "results_default"  # 默认输出根目录，将在主程序中根据模式设置
# LOG_FILE 将由 configure_logging 函数根据模式设置

# --- Rich Console (用于美化终端输出) ---
cs_console = Console(log_path=False)

# --- Global Constants (全局常量) ---
API_KEY = ""  # !!! 请替换为您的有效 Quake API Key !!!
BASE_URL = "https://quake.360.net/api/v3"  # Quake API 基础URL
INPUT_FILE = "icpCheck.txt"  # 默认输入文件，包含查询目标（公司名/关键词）
BATCH_SIZE = 1000  # Quake API每批次获取数量
DELAY = 3  # Quake API请求间隔（秒）
DEFAULT_PORTS = {  # fscan的默认扫描端口 (高级模式使用)
    80, 443, 8080, 8443, 3306, 22, 21, 23, 25, 110, 139, 445,
    1433, 1521, 3389, 5432, 6379, 27017
}
QUAKE_QUERY_TEMPLATE = 'icp_keywords:"{target}" and not domain_is_wildcard:true and country:"China" AND not province:"Hongkong"'
SHOW_SCAN_INFO = False  # <--- 新增：默认不显示扫描工具的实时输出


# ======================= 日志配置函数 =======================
def configure_logging(log_file_path):
    """
    配置日志记录器。
    确保日志处理器不重复添加，避免在重复调用时产生重复的日志记录。
    针对 Python 3.8 及更早版本，需要手动设置 FileHandler 的编码。
    """
    # 获取根 logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # 设置日志记录级别

    # 移除所有已存在的根日志处理器，防止重复记录
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 创建一个 FileHandler，并指定编码为 utf-8
    # 'w'模式表示覆盖写入日志文件
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')

    # 定义日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # 将配置好的 FileHandler 添加到 logger
    logger.addHandler(file_handler)

    # （可选）如果你也想在控制台看到INFO级别以上的日志（除了rich的输出外）
    # 可以取消注释下面的代码来添加一个 StreamHandler
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.INFO) # 或者你希望的级别
    # console_handler.setFormatter(formatter)
    # logger.addHandler(console_handler)

    logging.info(f"日志已配置，将记录到: {log_file_path} (使用UTF-8编码)")


# ======================= Shared Helper Functions (通用辅助函数) =======================
def load_queries(file_path):
    """从指定文件加载查询目标列表。"""
    logging.info(f"开始加载查询目标，文件路径: {file_path}")
    if not os.path.exists(file_path):
        logging.error(f"输入文件不存在: {file_path}")
        cs_console.print(f"[bold red]Error:[/bold red] 输入文件不存在: {file_path}")
        return []
    if os.path.getsize(file_path) == 0:
        logging.warning(f"输入文件为空: {file_path}")
        cs_console.print(f"[yellow]Warning:[/yellow] 输入文件为空: {file_path}")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]
    queries = [q for q in queries if q]
    logging.info(f"加载了 {len(queries)} 个有效的查询目标")
    cs_console.print(f"[green]INFO:[/green] 加载了 {len(queries)} 个有效的查询目标从 '{file_path}'")
    return queries


def query_all_pages(target):
    """使用Quake API滚动查询获取指定目标的所有相关服务数据。"""
    headers = {"X-QuakeToken": API_KEY, "Content-Type": "application/json"}
    current_query = QUAKE_QUERY_TEMPLATE.format(target=target)  # 使用全局模板
    logging.info(f"构造的Quake查询语句 (目标: {target}): {current_query}")
    base_query_params = {"query": current_query, "size": BATCH_SIZE, "ignore_cache": True, "latest": True}
    all_results = [];
    pagination_id = None;
    page_count = 0
    while True:
        page_count += 1
        query_params = base_query_params.copy()
        if pagination_id: query_params["pagination_id"] = pagination_id
        logging.debug(f"查询Quake API: {target}, page: {page_count}, pagination_id: {pagination_id}")
        try:
            response = requests.post(f"{BASE_URL}/scroll/quake_service", headers=headers, json=query_params, timeout=30)
            response.raise_for_status()
            result = response.json()
            if result["code"] != 0:
                logging.error(f"Quake API 查询失败 (目标: {target}): {result.get('message')}")
                cs_console.print(f"[bold red]Error:[/bold red] Quake API 查询失败 ({target}): {result.get('message')}")
                break
            current_data = result.get("data", [])
            if not current_data:
                logging.info(f"Quake API 查询完成 (目标: {target}), 第 {page_count} 页无数据, 已到最后一页")
                break
            all_results.extend(current_data)
            pagination_id = result.get("meta", {}).get("pagination_id")
            if not pagination_id:
                logging.info(f"Quake API 查询完成 (目标: {target}), 无更多分页ID")
                break
            time.sleep(DELAY)
        except requests.exceptions.RequestException as e:
            logging.error(f"Quake API 请求异常 (目标: {target}): {e}")
            cs_console.print(f"[bold red]Error:[/bold red] Quake API 请求异常 ({target}): {str(e)}")
            break
        except Exception as e:
            logging.error(f"Quake API 处理中发生未知异常 (目标: {target}): {e}", exc_info=True)
            cs_console.print(f"[bold red]Error:[/bold red] Quake API 未知异常 ({target}): {str(e)}")
            break
    logging.info(f"Quake API 查询结束 (目标: {target}), 共获取 {len(all_results)} 条记录, 查询了 {page_count} 页.")
    return all_results


def parse_results(raw_data_list):
    """解析从Quake API获取的原始数据列表，提取关键信息并结构化。"""
    parsed_results = []
    for raw_data in raw_data_list:
        service_info = raw_data.get("service", {});
        http_info = service_info.get("http", {})
        icp_info = http_info.get("icp", {});
        main_icp = icp_info.get("main_licence", {})
        location_info = raw_data.get("location", {});
        urls = http_info.get("http_load_url", [])
        url = urls[0] if urls else "";
        raw_title = http_info.get("title", "")
        clean_title = raw_title.replace("\n", " ").strip();
        ip = raw_data.get("ip", "")
        port = str(raw_data.get("port", ""));
        unit = main_icp.get("unit", "")
        province = location_info.get("province_cn", "")
        parsed = {"IP": ip, "Port": port, "Host": http_info.get("host", ""),
                  "HTTP状态码": http_info.get("status_code", ""), "URL": url,
                  "Domain": raw_data.get("domain", ""), "网站标题": clean_title,
                  "备案号": icp_info.get("licence", ""), "主体单位": unit,
                  "备案单位类型": main_icp.get("nature", ""), "时间": raw_data.get("time", ""),
                  "归属省份": province}
        parsed_results.append(parsed)
    return parsed_results


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


def write_quake_results_to_excel(company_name, data, stage=""):
    """将Quake查询的解析结果保存到Excel文件。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    company_dir = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(company_name))
    os.makedirs(company_dir, exist_ok=True)
    excel_path = os.path.join(company_dir, f"quake_result{filename_suffix}.xlsx")
    df = pd.DataFrame(data)
    try:
        df.to_excel(excel_path, index=False, sheet_name="Quake_Data")
        logging.info(f"Quake 查询结果已保存到: {excel_path} (共 {len(df)} 条)")
        cs_console.print(
            f"    [green]Success:[/green] Quake Excel 已保存: '{os.path.basename(excel_path)}' ({len(df)} 条)")
    except Exception as e:
        logging.error(f"保存Quake Excel失败 ({excel_path}): {e}", exc_info=True)
        cs_console.print(f"    [bold red]Error:[/bold red] 保存Quake Excel失败: {os.path.basename(excel_path)}")


def write_ips_to_file(company_name, company_dir_path, ip_list, stage=""):
    """将IP地址列表保存到文本文件，并进行C段分析。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    ip_list_file = os.path.join(company_dir_path, f"ips{filename_suffix}.txt")
    valid_ips = set()
    for ip_addr in ip_list:
        if isinstance(ip_addr, str) and ip_addr.count('.') == 3:
            try:
                parts = ip_addr.split('.')
                if all(0 <= int(p) <= 255 for p in parts): valid_ips.add(ip_addr)
            except ValueError:
                logging.warning(f"向ip文件写入时，发现无效的IP格式: {ip_addr} (公司: {company_name})，已跳过。")
        elif ip_addr:
            logging.warning(
                f"向ip文件写入时，发现非字符串或格式不符的IP值: {ip_addr} ({type(ip_addr)}) (公司: {company_name})，已跳过。")
    if not valid_ips:
        logging.info(f"IP列表为空或无效 (公司: {company_name})，不创建ip文件。")
        return None
    sorted_ips = sorted(list(valid_ips));
    c_segments = defaultdict(int)
    for ip_val in sorted_ips:
        parts = ip_val.split('.')
        if len(parts) == 4: c_segments[f"{parts[0]}.{parts[1]}.{parts[2]}"] += 1
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
            logging.info(f"IP 列表中发现 {c_segment_info_count} 个可能存在的 C 段 (公司: {company_name})。")
            cs_console.print(f"      [blue]INFO:[/blue] 发现 {c_segment_info_count} 个可能C段 (详情见文件)")
    except Exception as e:
        logging.error(f"保存IP列表文件失败 ({ip_list_file}): {e}", exc_info=True)
        cs_console.print(f"    [bold red]Error:[/bold red] 保存IP列表文件失败: {os.path.basename(ip_list_file)}")
    return ip_list_file


def write_urls_to_txt_file(company_name, company_dir_path, url_list, stage=""):
    """将URL列表保存到文本文件。"""
    filename_suffix = generate_filename_suffix(company_name, stage)
    url_list_file = os.path.join(company_dir_path, f"extracted_urls{filename_suffix}.txt")
    valid_urls = {str(u).strip() for u in url_list if u and str(u).strip()}
    if not valid_urls:
        logging.info(f"过滤后URL列表为空 (公司: {company_name}, 阶段: {stage})，不创建URL文件。")
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
        cs_console.print(
            f"    [bold red]Error:[/bold red] 保存提取的URL列表文件失败: {os.path.basename(url_list_file)}")
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
    """将指定目录下所有公司子目录中的CSV文件转换为Excel，并进行分表。"""
    cs_console.print(f"\n[green]INFO:[/green] 开始最终的CSV到Excel批量转换...")
    logging.info(f"开始最终的CSV到Excel批量转换处理，根目录: {output_base_dir_param}")
    processed_files_count = 0;
    deleted_csv_count = 0;
    error_files_count = 0
    if not os.path.exists(output_base_dir_param):
        logging.warning(f"输出根目录 {output_base_dir_param} 不存在，跳过CSV处理。")
        cs_console.print(f"  [yellow]Warning:[/yellow] 输出根目录 '{output_base_dir_param}' 不存在，跳过CSV处理。")
        return

    for company_dir_name in os.listdir(output_base_dir_param):
        company_path = os.path.join(output_base_dir_param, company_dir_name)
        if os.path.isdir(company_path):
            logging.info(f"CSV处理 - 检查公司目录: {company_path}")
            for filename in os.listdir(company_path):
                if filename.endswith(".csv"):
                    csv_file_path = os.path.join(company_path, filename)
                    excel_file_name = os.path.splitext(filename)[0] + ".xlsx"
                    excel_file_path = os.path.join(company_path, excel_file_name)
                    logging.info(f"CSV处理 - 准备转换: {csv_file_path} -> {excel_file_path}")
                    try:
                        df_csv = None
                        try:
                            df_csv = pd.read_csv(csv_file_path, encoding='utf-8-sig')
                        except UnicodeDecodeError:
                            try:
                                df_csv = pd.read_csv(csv_file_path, encoding='gbk')
                            except UnicodeDecodeError:
                                try:
                                    df_csv = pd.read_csv(csv_file_path, encoding='latin1')
                                except Exception as read_e:
                                    logging.error(f"Pandas读取CSV ({csv_file_path}) 尝试多种编码失败: {read_e}",
                                                  exc_info=True)
                                    cs_console.print(
                                        f"  [bold red]Error:[/bold red] 读取CSV '{os.path.basename(csv_file_path)}' 失败.")
                                    error_files_count += 1;
                                    continue
                        if df_csv is None or df_csv.empty:
                            logging.warning(f"CSV文件为空或读取失败: {csv_file_path}，跳过并尝试删除。")
                            cs_console.print(
                                f"  [yellow]Warning:[/yellow] CSV '{os.path.basename(csv_file_path)}' 为空或读取失败，将尝试删除。")
                            try:
                                os.remove(csv_file_path);
                                deleted_csv_count += 1
                            except OSError as e_remove:
                                logging.error(f"删除空的/无法读取的CSV文件 {csv_file_path} 失败: {e_remove}")
                            continue
                        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                            df_csv.to_excel(writer, sheet_name="原始数据", index=False)
                            if 'status_code' in df_csv.columns:
                                df_copy = df_csv.copy();
                                df_copy['status_code'] = pd.to_numeric(df_copy['status_code'], errors='coerce').fillna(
                                    0).astype(int)
                                valid_status_codes = [200, 301, 302];
                                df_valid = df_copy[df_copy['status_code'].isin(valid_status_codes)];
                                df_invalid = df_copy[~df_copy['status_code'].isin(valid_status_codes)]
                                if not df_valid.empty: df_valid.to_excel(writer, sheet_name="有效表", index=False)
                                if not df_invalid.empty: df_invalid.to_excel(writer, sheet_name="无效表", index=False)
                                logging.info(
                                    f"CSV转换成功并分表: {excel_file_path} (有效: {len(df_valid)}, 无效: {len(df_invalid)})")
                            else:
                                logging.warning(f"CSV文件 {filename} 中缺少 'status_code' 列，无法分表。")
                        processed_files_count += 1
                        try:
                            os.remove(csv_file_path);
                            deleted_csv_count += 1
                        except OSError as e_remove:
                            logging.error(f"删除CSV文件 {csv_file_path} 失败: {e_remove}")
                            cs_console.print(
                                f"  [yellow]Warning:[/yellow] 删除原始CSV '{os.path.basename(csv_file_path)}' 失败.")
                    except FileNotFoundError:
                        logging.error(f"CSV处理时文件未找到: {csv_file_path}");
                        cs_console.print(
                            f"  [bold red]Error:[/bold red] CSV文件未找到: {os.path.basename(csv_file_path)}");
                        error_files_count += 1
                    except pd.errors.EmptyDataError:
                        logging.warning(
                            f"Pandas读取CSV时发现文件为空 (EmptyDataError): {csv_file_path}，跳过。");
                        cs_console.print(
                            f"  [yellow]Warning:[/yellow] CSV '{os.path.basename(csv_file_path)}' 为空 (Pandas EmptyDataError)。")
                    except Exception as e:
                        logging.error(f"处理CSV文件 {csv_file_path} 失败: {e}", exc_info=True);
                        cs_console.print(
                            f"  [bold red]Error:[/bold red] 处理CSV '{os.path.basename(csv_file_path)}' 失败 (详情见日志).");
                        error_files_count += 1
    cs_console.print(f"[green]INFO:[/green] CSV到Excel转换处理完成:")
    cs_console.print(f"  成功转换: {processed_files_count} 个文件")
    cs_console.print(f"  成功删除原始CSV: {deleted_csv_count} 个文件")
    if error_files_count > 0: cs_console.print(
        f"  [yellow]处理失败:[/yellow] {error_files_count} 个文件 (详情请查看日志)")
    logging.info(
        f"所有CSV到Excel的最终转换处理完成。成功转换: {processed_files_count}, 成功删除CSV: {deleted_csv_count}, 转换/删除失败: {error_files_count}")


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
            port_num = int(p);
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
            tf.write(','.join(map(str, sorted_ports)));
            temp_file_path = tf.name  # This is an absolute path
        logging.info(f"端口列表写入临时文件: {temp_file_path} ({len(sorted_ports)}个)")
        return temp_file_path
    except Exception as e:
        logging.error(f"写入端口临时文件失败: {e}", exc_info=True);
        cs_console.print(
            f"    [bold red]Error:[/bold red] 写入端口临时文件失败: {e}");
        return None


def fscan_start(company_name, iplist_file_path_param, port_file_path_param=None):
    """运行fscan进行端口和服务扫描。根据全局 SHOW_SCAN_INFO 控制输出。
       确保传递给工具的路径是绝对路径。
    """
    global SHOW_SCAN_INFO
    filename_suffix = generate_filename_suffix(company_name, "fscan")
    # company_dir_path refers to the specific company's output directory.
    # It's os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(company_name))
    # This might be relative if OUTPUT_BASE_DIR is relative.
    company_dir_path_rel = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(company_name))
    # Ensure the company-specific output directory is absolute and exists
    absolute_company_dir_path = os.path.abspath(company_dir_path_rel)
    os.makedirs(absolute_company_dir_path, exist_ok=True)

    absolute_iplist_file_path = None
    if iplist_file_path_param:  # Check if it's not None or empty
        if os.path.exists(iplist_file_path_param):
            absolute_iplist_file_path = os.path.abspath(iplist_file_path_param)
        else:  # Path provided but file doesn't exist
            logging.error(f"fscan的IP列表文件未找到: {iplist_file_path_param} ({company_name})")
            cs_console.print(
                f"    [bold red]Error:[/bold red] fscan IP列表文件未找到: {os.path.basename(iplist_file_path_param or 'N/A')} ({company_name})")
            return None
    else:  # No IP list file path provided
        logging.error(f"fscan需要IP列表文件，但未提供 ({company_name})")
        cs_console.print(
            f"    [bold red]Error:[/bold red] fscan需要IP列表文件，但未提供 ({company_name})")
        return None

    # Output file path for fscan (absolute)
    absolute_output_file_path = os.path.join(absolute_company_dir_path, f"fscan_output{filename_suffix}.txt")

    absolute_port_file_path = None
    if port_file_path_param:  # Check if it's not None or empty
        # write_ports_to_temp_file already returns an absolute path for port_file_path_param
        if os.path.exists(port_file_path_param):
            absolute_port_file_path = os.path.abspath(port_file_path_param)  # Still good to ensure it's absolute
        else:  # Path provided but file doesn't exist
            logging.warning(f"指定端口文件 {port_file_path_param} 不存在，fscan将用内置端口。")
            cs_console.print(
                f"    [yellow]Warning:[/yellow] 端口文件 {os.path.basename(port_file_path_param or 'N/A')} 不存在，fscan用内置端口。")
            # absolute_port_file_path remains None
    # If port_file_path_param was None, absolute_port_file_path remains None, which is handled later

    logging.info(
        f"开始fscan: {company_name}, IP列表: {absolute_iplist_file_path}, 端口列表: {absolute_port_file_path if absolute_port_file_path else '默认'}")
    cs_console.print(f"    [blue]执行:[/blue] fscan 端口与服务扫描...")
    if SHOW_SCAN_INFO: cs_console.print(f"      [grey50](fscan 实时输出已开启...)[/grey50]")

    script_dir = os.path.dirname(os.path.abspath(__file__));
    tools_dir = os.path.join(script_dir, 'tools')
    fscan_exe_path = os.path.join(tools_dir, 'fscan64.exe')  # Absolute path to fscan

    if not os.path.exists(fscan_exe_path):
        logging.error(f"fscan64.exe 未找到: {fscan_exe_path}");
        cs_console.print(f"    [bold red]Error:[/bold red] fscan64.exe 未找到于 '{tools_dir}'.")
        return None

    # Construct command with absolute paths
    command = [fscan_exe_path, '-hf', absolute_iplist_file_path, '-t', '1600', '-np', '-nopoc', '-nobr', '-o',
               absolute_output_file_path]
    if not SHOW_SCAN_INFO: command.append('-silent')
    if absolute_port_file_path: command.extend(['-portf', absolute_port_file_path])

    logging.info(f"执行 fscan 命令: {' '.join(command)} (工作目录: {tools_dir})")
    subprocess_kwargs_fscan = {"check": True, "cwd": tools_dir}  # Keep cwd as tools_dir
    if not SHOW_SCAN_INFO:
        subprocess_kwargs_fscan["capture_output"] = True;
        subprocess_kwargs_fscan["text"] = True
        subprocess_kwargs_fscan["encoding"] = 'utf-8';
        subprocess_kwargs_fscan["errors"] = 'ignore'
    # else: (no capture, output goes to console, encoding handled by console)

    try:
        process = subprocess.run(command, **subprocess_kwargs_fscan)
        if not SHOW_SCAN_INFO and process: logging.debug(f"fscan STDOUT: {process.stdout}\nSTDERR: {process.stderr}")
        logging.info(f"fscan 执行成功，输出文件: {absolute_output_file_path}")  # Log absolute path
        cs_console.print(
            f"      [green]Success:[/green] fscan扫描完成, 结果: '{os.path.basename(absolute_output_file_path)}'")
    except subprocess.CalledProcessError as e:
        logging.error(f"fscan 执行失败: {e}", exc_info=True)
        if not SHOW_SCAN_INFO and hasattr(e, 'stdout') and e.stdout: logging.error(f"fscan STDOUT on error: {e.stdout}")
        if not SHOW_SCAN_INFO and hasattr(e, 'stderr') and e.stderr: logging.error(f"fscan STDERR on error: {e.stderr}")
        cs_console.print(f"      [bold red]Error:[/bold red] fscan 执行失败 (详情见日志)。")
        return None  # Return None as output_file_abs was the original return
    except FileNotFoundError:
        logging.error(f"fscan64.exe 执行出错 (FileNotFoundError)。", exc_info=True);
        cs_console.print(
            f"      [bold red]Error:[/bold red] fscan64.exe 执行出错 (FileNotFoundError)。");
        return None
    except Exception as e:
        logging.error(f"fscan 执行时发生未知错误: {e}", exc_info=True);
        cs_console.print(
            f"      [bold red]Error:[/bold red] fscan 未知错误 (详情见日志).");
        return None
    # Removed os.chdir logic

    if absolute_output_file_path and (
            not os.path.exists(absolute_output_file_path) or os.path.getsize(absolute_output_file_path) == 0):
        logging.warning(f"fscan 输出文件为空或未生成: {absolute_output_file_path}")
        cs_console.print(
            f"      [yellow]Warning:[/yellow] fscan 输出文件为空或未生成: {os.path.basename(absolute_output_file_path or 'N/A')}")
    return absolute_output_file_path  # Return the absolute path


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

    def saveFile(self, unused_output_dir_param,
                 stage=""):  # unused_output_dir_param is not actually used here for path construction
        """将解析后的fscan数据保存到Excel文件的不同工作表中。"""
        filename_suffix = generate_filename_suffix(self.company_name, stage)
        # Results are saved relative to OUTPUT_BASE_DIR/company_name/
        # Ensure company_dir is absolute for consistency if needed, but os.path.join works with relative OUTPUT_BASE_DIR fine
        company_dir_rel = os.path.join(OUTPUT_BASE_DIR, sanitize_sheet_name(self.company_name))
        absolute_company_dir = os.path.abspath(company_dir_rel)  # Make it absolute
        os.makedirs(absolute_company_dir, exist_ok=True)  # Ensure it exists

        fileName = os.path.join(absolute_company_dir, f"fscan_result{filename_suffix}.xlsx")

        def format_file_size(size_bytes):
            if size_bytes == 0: return "0 B"
            size_name = ("B", "KB", "MB", "GB", "TB")
            if abs(size_bytes) < 1:  # handles values between 0 and 1 (exclusive of 0)
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
            p = pow(1024, i);
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
def run_basic_mode():
    """执行基础模式的扫描和信息收集流程。"""
    start_time_basic = time.time()
    cs_console.print(f"[bold blue]基础模式启动...[/bold blue]")
    logging.info(f"基础模式: 核心流程开始")

    targets = load_queries(INPUT_FILE)
    if not targets: return

    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)  # OUTPUT_BASE_DIR can be relative here
    all_company_assets = defaultdict(lambda: {"ips": set(), "urls": set(), "raw_data": []})

    cs_console.print(f"\n[bold blue]阶段一 (Basic): Quake数据查询[/bold blue] (共 {len(targets)} 个目标)")
    for index, target in enumerate(targets, 1):
        cs_console.print(f"  ({index}/{len(targets)}) 查询Quake API: '{target}'...")
        logging.info(f"Basic: 开始处理查询目标: {target} ({index}/{len(targets)})")
        raw_quake_data_list = query_all_pages(target)
        if raw_quake_data_list:
            parsed_quake_results = parse_results(raw_quake_data_list)
            if not parsed_quake_results:
                logging.warning(f"Basic: 目标 '{target}' 的Quake原始数据解析后为空。")
                continue
            for item in parsed_quake_results:
                company_name = item.get("主体单位")
                ip_addr = item.get("IP");
                url = item.get("URL")
                key_name = company_name if company_name else "未知主体单位"
                all_company_assets[key_name]["raw_data"].append(item)
                if ip_addr: all_company_assets[key_name]["ips"].add(ip_addr)
                if url: all_company_assets[key_name]["urls"].add(url)
                if not company_name and (url or ip_addr):
                    logging.warning(
                        f"Basic: Quake记录缺少主体单位信息, IP: {ip_addr or 'N/A'}, URL: {url or 'N/A'}. 已归类到 '{key_name}'")
            cs_console.print(f"    [green]完成:[/green] '{target}' 查询到 {len(raw_quake_data_list)} 条原始记录.")
        else:
            logging.warning(f"Basic: Quake API 查询无结果，目标: {target}")
            cs_console.print(f"    [yellow]无结果:[/yellow] 目标 '{target}' 无Quake查询结果.")

    if not all_company_assets:
        cs_console.print("\n[yellow]INFO (Basic):[/yellow] 未从Quake API获取到任何有效资产，流程结束。")
        logging.info("Basic: 未从Quake API获取到任何有效资产。")
        process_all_generated_csvs(OUTPUT_BASE_DIR)
        return
    cs_console.print(
        f"\n[green]INFO (Basic):[/green] Quake查询完毕, 共收集到 {len(all_company_assets)} 个公司/主体的资产信息。")

    cs_console.print(f"\n[bold blue]阶段二 (Basic): 公司资产处理与指纹识别[/bold blue]")
    company_processed_count = 0
    for company_name, assets in all_company_assets.items():
        company_processed_count += 1
        sanitized_company_dir_name = sanitize_sheet_name(company_name)
        # company_dir is relative if OUTPUT_BASE_DIR is relative
        company_dir = os.path.join(OUTPUT_BASE_DIR, sanitized_company_dir_name)
        os.makedirs(company_dir, exist_ok=True)  # This creates the directory relative to script execution
        cs_console.print(
            f"\n  ({company_processed_count}/{len(all_company_assets)}) 处理公司: [cyan]{company_name}[/cyan]")
        logging.info(f"Basic: 开始处理公司: {company_name}")

        if assets["raw_data"]: write_quake_results_to_excel(company_name, assets["raw_data"], stage="quake_data")
        if assets["ips"]: write_ips_to_file(company_name, company_dir, list(assets["ips"]), stage="quake_ips")

        all_urls_for_company = list(assets["urls"])
        general_url_list_file_path = None
        if all_urls_for_company:
            # general_url_list_file_path will be relative if company_dir is relative
            general_url_list_file_path = write_urls_to_txt_file(company_name, company_dir, all_urls_for_company,
                                                                stage="quake_urls")
            cs_console.print(f"    [blue]INFO:[/blue] 找到 {len(all_urls_for_company)} 个URL，准备进行指纹识别。")

            if general_url_list_file_path:
                # Pass company_dir (which might be relative) and general_url_list_file_path (also might be relative)
                # run_observer_ward will convert them to absolute paths internally.
                run_observer_ward(company_name, company_dir, None, "fingerprint_from_quake",
                                  url_list_file_path=general_url_list_file_path)
            else:
                cs_console.print(f"    [yellow]INFO:[/yellow] 通用URL列表文件未创建，跳过指纹识别。")
        else:
            cs_console.print(f"    [yellow]INFO:[/yellow] 无URL信息，跳过URL文件写入和指纹识别。")

        move_txt_to_related_materials(company_dir, company_name)

    logging.info("Basic: 所有公司特定目标处理完成。")
    cs_console.print(f"\n[green]INFO (Basic):[/green] 所有公司特定目标处理完成。")

    cs_console.print(f"\n[bold blue]阶段三 (Basic): CSV结果转换[/bold blue]")
    process_all_generated_csvs(OUTPUT_BASE_DIR)  # OUTPUT_BASE_DIR can be relative

    end_time_basic = time.time()
    total_duration_basic = end_time_basic - start_time_basic
    cs_console.print(f"\n[bold green]基础模式结束.[/bold green] 总耗时: {total_duration_basic:.2f} 秒.")
    logging.info(f"基础模式结束. 总耗时: {total_duration_basic:.2f} 秒.")


# ======================= Advanced Mode Main Logic (高级模式主逻辑) =======================
def run_advanced_mode():
    """执行高级模式的扫描和信息收集流程，包含fscan等。"""
    start_time_advanced = time.time();
    cs_console.print(f"[bold blue]高级模式启动...[/bold blue]");
    logging.info(f"高级模式: 核心流程开始")
    targets = load_queries(INPUT_FILE)
    if not targets: return
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)  # OUTPUT_BASE_DIR can be relative
    all_quake_results_by_company = defaultdict(lambda: {"ip": set(), "allPort": set(), "urls": set(), "raw_data": []})
    cs_console.print(f"\n[bold blue]阶段一 (Advanced): Quake数据查询[/bold blue] (共 {len(targets)} 个目标)")
    for index, target in enumerate(targets, 1):
        cs_console.print(f"  ({index}/{len(targets)}) 查询Quake API: '{target}'...");
        logging.info(f"Advanced: 处理目标: {target} ({index}/{len(targets)})")
        raw_data = query_all_pages(target)
        if raw_data:
            parsed = parse_results(raw_data)
            for item in parsed:
                company_name = item.get("主体单位");
                url_item = item.get("URL");
                ip_addr = item.get("IP");
                port_val = item.get("Port")
                key_name = company_name if company_name else "未知主体单位_Advanced"
                all_quake_results_by_company[key_name]["raw_data"].append(item)
                if ip_addr: all_quake_results_by_company[key_name]["ip"].add(ip_addr)
                if port_val: all_quake_results_by_company[key_name]["allPort"].add(port_val)
                if url_item: all_quake_results_by_company[key_name]["urls"].add(url_item)
                if not company_name and (url_item or ip_addr): logging.warning(
                    f"Advanced: Quake记录缺主体单位, IP: {ip_addr or 'N/A'}, URL: {url_item or 'N/A'}. 归类到 '{key_name}'")
            cs_console.print(f"    [green]完成:[/green] '{target}' 查询到 {len(raw_data)} 条原始记录.")
        else:
            logging.warning(f"Advanced: Quake查询无结果，目标: {target}");
            cs_console.print(
                f"    [yellow]无结果:[/yellow] 目标 '{target}' 无Quake查询结果.")
    if not all_quake_results_by_company:
        cs_console.print("\n[yellow]INFO (Advanced):[/yellow] 未从Quake获取任何公司数据，流程结束。")
        logging.info("Advanced: 未从Quake获取任何公司数据。")
        process_all_generated_csvs(OUTPUT_BASE_DIR)
        return
    cs_console.print(
        f"\n[green]INFO (Advanced):[/green] Quake查询完毕, 共收集到 {len(all_quake_results_by_company)} 个公司/主体的资产信息。")

    cs_console.print(f"\n[bold blue]阶段二 (Advanced): 公司资产处理、扫描与指纹识别[/bold blue]")
    company_processed_count_adv = 0
    for company_name, company_data in all_quake_results_by_company.items():
        company_processed_count_adv += 1;
        sanitized_company_dir_name = sanitize_sheet_name(company_name)
        # company_dir is relative if OUTPUT_BASE_DIR is relative
        company_dir = os.path.join(OUTPUT_BASE_DIR, sanitized_company_dir_name);
        os.makedirs(company_dir, exist_ok=True)  # Creates dir relative to script execution
        cs_console.print(
            f"\n  ({company_processed_count_adv}/{len(all_quake_results_by_company)}) 处理公司: [cyan]{company_name}[/cyan]");
        logging.info(f"Advanced: 处理公司: {company_name}")

        if not company_data["raw_data"]:
            logging.info(f"Advanced: 跳过公司 {company_name}，无Quake数据。")
            cs_console.print(f"    [yellow]INFO:[/yellow] 公司 {company_name} 无Quake数据，跳过。")
            continue

        if company_data["raw_data"]: write_quake_results_to_excel(company_name, company_data["raw_data"],
                                                                  stage="quake_initial")

        ip_list_file_path = None  # This will be relative if company_dir is relative
        if company_data["ip"]:
            ip_list_file_path = write_ips_to_file(company_name, company_dir, list(company_data["ip"]),
                                                  stage="initial_ips")
        else:
            cs_console.print(f"    [yellow]INFO:[/yellow] 公司 {company_name} 无IP信息，跳过IP文件和fscan。");
            logging.info(f"Advanced: 公司 {company_name} 无IP信息，跳过IP文件和fscan。")

        # temp_port_file_path from write_ports_to_temp_file is absolute
        temp_port_file_path = write_ports_to_temp_file(list(company_data["allPort"])) if company_data[
            "allPort"] else write_ports_to_temp_file([])
        if not company_data["allPort"]:
            cs_console.print(f"    [yellow]INFO:[/yellow] 公司 {company_name} Quake无端口信息，fscan用默认端口。")
            logging.info(f"Advanced: 公司 {company_name} Quake无端口信息，fscan用默认端口。")

        if company_data["urls"]:
            # Pass company_dir (relative) and urls, run_observer_ward handles path absolutization
            run_observer_ward(company_name, company_dir, list(company_data["urls"]), "initial_quake_urls")
        else:
            cs_console.print(f"    [yellow]INFO:[/yellow] 公司 {company_name} Quake无URL，跳过初次指纹识别。")

        fscan_output_file_path = None  # Will be absolute if fscan_start succeeds
        if ip_list_file_path:  # ip_list_file_path might be relative
            fscan_output_file_path = fscan_start(company_name, ip_list_file_path, temp_port_file_path)
        else:
            cs_console.print(f"    [yellow]INFO:[/yellow] 公司 {company_name} 无IP列表文件，跳过fscan。")

        fscan_discovered_urls = set();
        excel_file_from_fscan = None
        if fscan_output_file_path and os.path.exists(fscan_output_file_path) and os.path.getsize(
                fscan_output_file_path) > 0:
            # fscan_output_file_path is absolute here
            # company_dir is relative, but process_fscan_output_advanced uses it for beautify.run,
            # and beautify.saveFile handles its own absolute pathing.
            fscan_discovered_urls, excel_file_from_fscan = process_fscan_output_advanced(fscan_output_file_path,
                                                                                         company_dir, company_name,
                                                                                         "fscan_processed")
        elif fscan_output_file_path:  # fscan ran but output was empty/not found (should be caught by fscan_start returning None though)
            cs_console.print(
                f"    [yellow]INFO:[/yellow] fscan输出文件 '{os.path.basename(fscan_output_file_path)}' 为空或未找到，跳过处理。");
            logging.info(
                f"Advanced: fscan输出 {fscan_output_file_path} 为空或未找到，跳过处理。")

        if fscan_discovered_urls:
            cs_console.print(
                f"    [blue]INFO:[/blue] fscan为 {company_name} 发现 {len(fscan_discovered_urls)} 个新URL，二次指纹识别。");
            # Pass company_dir (relative) and urls, run_observer_ward handles path absolutization
            run_observer_ward(
                company_name, company_dir, list(fscan_discovered_urls), "fscan_discovered_urls")
        else:
            logging.info(f"Advanced: fscan未为 {company_name} 发现新URL，跳过二次指纹。");
            cs_console.print(
                f"    [yellow]INFO:[/yellow] fscan未发现新URL，跳过二次指纹。")

        if temp_port_file_path and os.path.exists(temp_port_file_path):  # temp_port_file_path is absolute
            try:
                os.remove(temp_port_file_path);
                logging.info(f"Advanced: 已删临时端口文件: {temp_port_file_path}")
            except OSError as e:
                logging.warning(f"Advanced: 删除临时端口文件 {temp_port_file_path} 失败: {e}")

        move_txt_to_related_materials(company_dir, company_name)  # company_dir is relative

    logging.info("Advanced: 所有公司处理完成。");
    cs_console.print(f"\n[green]INFO (Advanced):[/green] 所有公司处理完成。")
    cs_console.print(f"\n[bold blue]阶段三 (Advanced): CSV结果转换[/bold blue]");
    process_all_generated_csvs(OUTPUT_BASE_DIR)  # OUTPUT_BASE_DIR can be relative

    end_time_advanced = time.time();
    total_duration_advanced = end_time_advanced - start_time_advanced
    cs_console.print(f"\n[bold green]高级模式结束.[/bold green] 总耗时: {total_duration_advanced:.2f} 秒.");
    logging.info(f"高级模式结束. 总耗时: {total_duration_advanced:.2f} 秒.")


# ======================= Main Execution Logic (主执行逻辑) =======================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ICP Asset Express - 基于Quake和外部工具的信息收集与扫描脚本",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=f"""
    使用示例:
      python {os.path.basename(__file__)} -a -i my_targets.txt -o ./my_scan_results
      python {os.path.basename(__file__)} --basic --apikey YOUR_ACTUAL_API_KEY
      python {os.path.basename(__file__)} --advanced --showScanInfo

    默认行为:
      如果不指定模式 (-a 或 -b)，脚本将默认以高级模式运行。
      输入文件默认为 '{INPUT_FILE}'。
      输出目录默认为 'results_icp_basic' (基础模式) 或 'results_icp_advanced' (高级模式)。
    """
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '-b', '--basic',
        action='store_true',
        help="运行基础模式: Quake查询 -> 提取IP/URL到txt -> 对Quake URL进行指纹识别。"
    )
    mode_group.add_argument(
        '-a', '--advanced',
        action='store_true',
        help="运行高级模式 (默认): Quake查询 -> IP/URL/端口提取 -> 初次指纹识别 -> fscan扫描 -> fscan结果处理 -> 二次指纹识别。"
    )
    parser.add_argument(
        '-i', '--input',
        type=str,
        help=f"指定包含查询目标（公司名/关键词）的输入文件名。\n每行一个目标。默认为: '{INPUT_FILE}'。"
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help="指定自定义的输出根目录。\n脚本会在此目录下根据模式自动添加后缀 (如 '_basic' 或 '_advanced')，\n并在其中为每个目标公司创建子目录。\n如果未指定，将使用模式默认的输出目录 (例如 'results_icp_advanced')。"
    )
    parser.add_argument(
        '--apikey',
        type=str,
        help=f"指定360 Quake API Key。\n如果提供，将覆盖脚本中预设的 API_KEY 值。"
    )
    parser.add_argument(
        '--showScanInfo',
        action='store_true',
        help="显示外部扫描工具 (如 fscan, observer_ward) 的实时运行输出。\n默认不显示 (静默执行)。"
    )

    args = parser.parse_args()

    if args.showScanInfo:
        SHOW_SCAN_INFO = True
        cs_console.print("[green]INFO:[/] 将显示扫描工具的实时输出。")
    else:
        SHOW_SCAN_INFO = False
        cs_console.print("[green]INFO:[/] 扫描工具的实时输出已关闭 (使用 --showScanInfo 显示)。")

    if args.input:
        INPUT_FILE = args.input
        cs_console.print(f"[green]INFO:[/] 输入文件已通过命令行设置为: '{INPUT_FILE}'")

    if args.apikey:
        API_KEY = args.apikey
        cs_console.print(f"[green]INFO:[/] Quake API Key 已通过命令行设置。")

    script_start_time = time.time()
    LOG_FILE = ""

    # Determine OUTPUT_BASE_DIR based on mode and -o argument
    # This OUTPUT_BASE_DIR can be relative (if -o is not used or is relative) or absolute.
    # Functions creating directories (like company_dir) will work relative to script execution path if OUTPUT_BASE_DIR is relative.
    # Functions calling external tools (run_observer_ward, fscan_start) now convert these potentially relative paths to absolute for the tool.
    if args.basic:
        cs_console.print("[bold underline green]启动基础模式 (Basic Mode)[/bold underline green]")
        LOG_FILE = "log_icp_basic.txt"
        OUTPUT_BASE_DIR = args.output if args.output else "results_icp_basic"
        configure_logging(LOG_FILE)
        run_basic_mode()
    elif args.advanced:
        cs_console.print("[bold underline green]启动高级模式 (Advanced Mode)[/bold underline green]")
        LOG_FILE = "log_icp_advanced.txt"
        OUTPUT_BASE_DIR = args.output if args.output else "results_icp_advanced"
        configure_logging(LOG_FILE)
        run_advanced_mode()
    else:  # Default to advanced
        cs_console.print("[bold underline green]未指定模式，默认启动高级模式 (Advanced Mode)[/bold underline green]")
        LOG_FILE = "log_icp_advanced.txt"
        OUTPUT_BASE_DIR = args.output if args.output else "results_icp_advanced"
        configure_logging(LOG_FILE)
        run_advanced_mode()

    script_end_time = time.time();
    overall_duration = script_end_time - script_start_time
    current_time_str_finally = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cs_console.print(f"\n[bold green]脚本整体执行完毕于: {current_time_str_finally}[/bold green]")
    cs_console.print(f"脚本整体运行时间: {overall_duration:.2f} 秒 ({datetime.timedelta(seconds=overall_duration)}).")
    logging.info(f"脚本整体执行完毕于: {current_time_str_finally}. 总耗时: {overall_duration:.2f} 秒.")
