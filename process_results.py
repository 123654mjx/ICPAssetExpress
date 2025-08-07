import os
import shutil
import pandas as pd
import argparse
from rich.console import Console
from rich.theme import Theme

# --- 初始化美化终端 ---
custom_theme = Theme({
    "info": "cyan",
    "success": "green",
    "warning": "yellow",
    "error": "bold red"
})
console = Console(theme=custom_theme)

def convert_csvs_in_tree(root_path: str):
    """
    递归遍历指定路径，将所有.csv文件转换为格式化的.xlsx文件。
    转换成功后删除原始的.csv文件。
    """
    console.print("\n--- [bold info]阶段一: 开始转换 .csv 文件为 .xlsx[/bold info] ---")
    converted_count = 0
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            if not filename.lower().endswith(".csv"):
                continue

            csv_path = os.path.join(dirpath, filename)
            excel_path = os.path.splitext(csv_path)[0] + ".xlsx"
            
            console.print(f"  [info]发现CSV文件:[/info] [cyan]{filename}[/cyan]")
            try:
                # 尝试用不同编码读取CSV，增加兼容性
                try:
                    df = pd.read_csv(csv_path, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    df = pd.read_csv(csv_path, encoding='gbk')

                if df.empty:
                    console.print(f"  [warning]警告: CSV文件为空，将直接删除: {filename}[/warning]")
                    os.remove(csv_path)
                    continue

                # 使用 openpyxl 引擎写入多工作表的Excel
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name="原始数据", index=False)
                    # 如果是指纹识别结果，则额外创建有效/无效表
                    if 'status_code' in df.columns:
                        df_copy = df.copy()
                        df_copy['status_code'] = pd.to_numeric(df_copy['status_code'], errors='coerce').fillna(0).astype(int)
                        valid_codes = [200, 301, 302]
                        df_valid = df_copy[df_copy['status_code'].isin(valid_codes)]
                        df_invalid = df_copy[~df_copy['status_code'].isin(valid_codes)]
                        
                        if not df_valid.empty:
                            df_valid.to_excel(writer, sheet_name="有效表", index=False)
                        if not df_invalid.empty:
                            df_invalid.to_excel(writer, sheet_name="无效表", index=False)

                os.remove(csv_path)
                console.print(f"  [success]转换成功:[/success] [green]{filename}[/green] -> [green]{os.path.basename(excel_path)}[/green]")
                converted_count += 1

            except Exception as e:
                console.print(f"  [error]错误: 处理文件 {filename} 失败: {e}[/error]")

    if converted_count == 0:
        console.print("[info]未发现需要转换的 .csv 文件。[/info]")

def archive_files_in_tree(root_path: str):
    """
    递归遍历指定路径，将所有.txt和.json文件移动到'related_materials'子目录中。
    """
    console.print("\n--- [bold info]阶段二: 开始归档 .txt 和 .json 中间文件[/bold info] ---")
    archived_count = 0
    extensions_to_move = ('.txt', '.json')

    for dirpath, _, filenames in os.walk(root_path):
        # 避免处理related_materials目录自身内部的文件
        if os.path.basename(dirpath) == 'related_materials':
            continue

        archive_dir = os.path.join(dirpath, 'related_materials')
        
        files_to_move = [f for f in filenames if f.lower().endswith(extensions_to_move)]
        
        if not files_to_move:
            continue

        os.makedirs(archive_dir, exist_ok=True)
        
        for filename in files_to_move:
            source_path = os.path.join(dirpath, filename)
            destination_path = os.path.join(archive_dir, filename)
            try:
                shutil.move(source_path, destination_path)
                console.print(f"  [success]归档成功:[/success] [cyan]{filename}[/cyan] -> [cyan]related_materials/[/cyan]")
                archived_count += 1
            except Exception as e:
                console.print(f"  [error]错误: 移动文件 {filename} 失败: {e}[/error]")

    if archived_count == 0:
        console.print("[info]未发现需要归档的 .txt 或 .json 文件。[/info]")


def merge_processed_excels(target_folder, output_folder):
    """
    合并所有已整理好的.xlsx文件。此函数逻辑与您提供的 merge 3.0.py 基本一致。
    """
    console.print("\n--- [bold info]阶段三: 开始合并所有 .xlsx 报告文件[/bold info] ---")
    
    # 这部分代码直接复用和优化您已有的 merge 3.0.py 逻辑
    report_data = {
        "gogo": {"原始表": [], "有效表": [], "无效表": []},
        "fingerprint": {"原始数据": [], "有效表": [], "无效表": []},
        "quake": [],
        "fofa": []
    }

    for root, _, files in os.walk(target_folder):
        for file in files:
            if not file.endswith(".xlsx"):
                continue

            file_path = os.path.join(root, file)
            
            try:
                if file.startswith("Gogo_Full_Report_"):
                    xls = pd.ExcelFile(file_path, engine="openpyxl")
                    for sheet_name in xls.sheet_names:
                        if sheet_name in report_data["gogo"]:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            df["来源文件名"] = file  # 添加来源信息
                            report_data["gogo"][sheet_name].append(df)

                elif file.startswith("url_fingerprint_"):
                    xls = pd.ExcelFile(file_path, engine="openpyxl")
                    for sheet_name in xls.sheet_names:
                        if sheet_name in report_data["fingerprint"]:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            df["来源文件名"] = file
                            report_data["fingerprint"][sheet_name].append(df)

                elif file.startswith("quake_result_"):
                    df = pd.read_excel(file_path, engine="openpyxl")
                    df["来源文件名"] = file
                    report_data["quake"].append(df)
                    
                elif file.startswith("fofa_results_"):
                    df = pd.read_excel(file_path, engine="openpyxl")
                    df["来源文件名"] = file
                    report_data["fofa"].append(df)

            except Exception as e:
                console.print(f"[warning]警告: 读取Excel文件失败: {file_path}, 错误: {e}[/warning]")

    os.makedirs(output_folder, exist_ok=True)
    console.print(f"\n[info]准备写入合并后的总表到目录:[/info] [cyan]{output_folder}[/cyan]")

    def write_and_format_excel(output_path, data_frames, wrap_columns=[]):
        try:
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                for sheet_name, frames in data_frames.items():
                    if not frames: continue
                    combined_df = pd.concat(frames, ignore_index=True)
                    combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    workbook, worksheet = writer.book, writer.sheets[sheet_name]
                    wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                    top_align_format = workbook.add_format({'valign': 'top'})
                    text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})

                    for col_num, name in enumerate(combined_df.columns):
                        width = max(combined_df[name].astype(str).map(len).max(), len(name)) + 2
                        cell_format = top_align_format
                        if name.lower() == 'url': cell_format = text_format
                        elif name in wrap_columns: cell_format = wrap_format
                        worksheet.set_column(col_num, col_num, min(width, 70), cell_format)
            
            console.print(f"[success]合并成功:[/success] [green]{os.path.basename(output_path)}[/green] 已保存。")
        except Exception as e:
            console.print(f"[error]错误: 写入合并文件 {os.path.basename(output_path)} 失败: {e}[/error]")

    # --- 调用写入函数 ---
    if any(report_data["gogo"].values()):
        write_and_format_excel(os.path.join(output_folder, "Gogo_Report_Merged.xlsx"), report_data["gogo"], ['title / banner', 'finger_name', 'Vulnerabilities'])
    if any(report_data["fingerprint"].values()):
        write_and_format_excel(os.path.join(output_folder, "Fingerprint_Merged.xlsx"), report_data["fingerprint"], ['title', 'finger'])
    if report_data["quake"]:
        write_and_format_excel(os.path.join(output_folder, "Quake_Result_Merged.xlsx"), {"Quake_Data": report_data["quake"]}, ['产品指纹', '网站标题'])
    if report_data["fofa"]:
        write_and_format_excel(os.path.join(output_folder, "Fofa_Result_Merged.xlsx"), {"Fofa_Data": report_data["fofa"]}, ['网站标题'])


def main():
    """主函数，用于处理命令行参数和调用核心功能。"""
    parser = argparse.ArgumentParser(
        description="""一个用于整理和合并 ICPAssetExpress 未完成结果的救援脚本。
它会自动将.csv转为.xlsx，归档.txt/.json文件，并合并所有报告。""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-t", "--target", 
        required=True, 
        help="包含所有目标结果的根文件夹路径 (例如 E:\\self\\众测\\青海资产)"
    )
    parser.add_argument(
        "-o", "--output", 
        required=True, 
        help="用于存放最终合并报告的输出文件夹路径"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.target):
        console.print(f"[error]错误: 目标路径 '{args.target}' 不是一个有效的目录。[/error]")
        return

    # 阶段一：转换CSV
    convert_csvs_in_tree(args.target)
    
    # 阶段二：归档文件
    archive_files_in_tree(args.target)

    # 阶段三：合并报告
    merge_processed_excels(args.target, args.output)

    console.print("\n[bold success]所有救援任务执行完毕！[/bold success]")

if __name__ == "__main__":
    main()