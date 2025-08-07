import os
import pandas as pd
import argparse

def merge_excels(target_folder, output_folder):
    """
    遍历目标文件夹，识别并合并指定前缀的Excel文件。
    Gogo和url_fingerprint报告将根据其内部的Sheet进行分类合并，并对合并后的文件进行格式化。
    """
    # 初始化用于存储不同类型DataFrame的列表
    report_data = {
        "gogo": {"原始表": [], "有效表": [], "无效表": []},
        "fingerprint": {"原始数据": [], "有效表": [], "无效表": []},
        "quake": [],
        "fofa": []
    }

    print(f"开始扫描目标文件夹: {target_folder}\n")
    for root, dirs, files in os.walk(target_folder):
        for file in files:
            if not file.endswith(".xlsx"):
                continue

            file_path = os.path.join(root, file)
            
            try:
                # --- 处理 Gogo 报告 (多Sheet) ---
                if file.startswith("Gogo_Full_Report_"):
                    print(f"正在处理 Gogo 报告: {file}")
                    xls = pd.ExcelFile(file_path, engine="openpyxl")
                    for sheet_name in xls.sheet_names:
                        if sheet_name in report_data["gogo"]:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            df["来源表名"] = file
                            report_data["gogo"][sheet_name].append(df)

                # --- 处理 url_fingerprint 报告 (多Sheet) ---
                elif file.startswith("url_fingerprint_"):
                    print(f"正在处理指纹报告: {file}")
                    xls = pd.ExcelFile(file_path, engine="openpyxl")
                    for sheet_name in xls.sheet_names:
                        if sheet_name in report_data["fingerprint"]:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            df["来源表名"] = file
                            report_data["fingerprint"][sheet_name].append(df)

                # --- 处理 Quake 和 Fofa 报告 (单Sheet) ---
                elif file.startswith("quake_result_"):
                    print(f"正在处理 Quake 报告: {file}")
                    df = pd.read_excel(file_path, engine="openpyxl")
                    df["来源表名"] = file
                    report_data["quake"].append(df)
                elif file.startswith("fofa_results_"):
                    print(f"正在处理 Fofa 报告: {file}")
                    df = pd.read_excel(file_path, engine="openpyxl")
                    df["来源表名"] = file
                    report_data["fofa"].append(df)

            except Exception as e:
                print(f"警告: 读取或处理文件失败: {file_path}, 错误: {e}")

    os.makedirs(output_folder, exist_ok=True)
    print(f"\n开始写入合并后的文件到目录: {output_folder}")

    # --- 统一的写入和格式化函数 ---
    def write_and_format_excel(output_path, data_frames, wrap_columns=[]):
        try:
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                for sheet_name, frames in data_frames.items():
                    if not frames: continue
                    combined_df = pd.concat(frames, ignore_index=True)
                    combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    workbook = writer.book
                    worksheet = writer.sheets[sheet_name]
                    wrap_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                    top_align_format = workbook.add_format({'valign': 'top'})
                    text_format = workbook.add_format({'num_format': '@', 'valign': 'top'})

                    for col_num, col_name in enumerate(combined_df.columns):
                        max_len = max(combined_df[col_name].astype(str).map(len).max(), len(col_name))
                        width = min(max_len + 5, 70)
                        
                        cell_format = None
                        if col_name == 'url' or col_name == 'URL':
                            cell_format = text_format
                        elif col_name in wrap_columns:
                            cell_format = wrap_format
                        
                        worksheet.set_column(col_num, col_num, width, cell_format)

                    for row_num in range(len(combined_df)):
                        max_lines = 1
                        for col_name in wrap_columns:
                            if col_name in combined_df.columns:
                                cell_value = str(combined_df.iloc[row_num][col_name])
                                num_lines = cell_value.count('\n') + 1
                                max_lines = max(max_lines, num_lines)
                        
                        if max_lines > 1:
                            worksheet.set_row(row_num + 1, max_lines * 15, wrap_format)
                        else:
                            worksheet.set_row(row_num + 1, None, top_align_format)
                
                print(f"文件 {os.path.basename(output_path)} 已成功保存。")
        except Exception as e:
            print(f"写入文件 {os.path.basename(output_path)} 失败: {e}")

    # --- 调用写入函数 ---
    if report_data["gogo"]["原始表"]:
        write_and_format_excel(
            os.path.join(output_folder, "Gogo_Full_Report_all.xlsx"),
            report_data["gogo"],
            ['title / banner', 'finger_name', 'finger_version', 'finger_vendor', 'finger_product', 'Vulnerabilities']
        )
    
    if report_data["fingerprint"]["原始数据"]:
        write_and_format_excel(
            os.path.join(output_folder, "url_fingerprint_all.xlsx"),
            report_data["fingerprint"],
            ['title', 'finger']
        )

    if report_data["quake"]:
        write_and_format_excel(
            os.path.join(output_folder, "quake_result_all.xlsx"),
            {"Quake_Data": report_data["quake"]},
            ['产品指纹', '网站标题']
        )

    if report_data["fofa"]:
        write_and_format_excel(
            os.path.join(output_folder, "fofa_results_all.xlsx"),
            {"Fofa_Data": report_data["fofa"]},
            ['网站标题']
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""合并 ICPAssetExpress Gogo版生成的各类Excel报告文件，并对合并后的总表进行格式化。""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-t", "--target", required=True, help="包含待合并Excel文件的目标文件夹路径")
    parser.add_argument("-o", "--output", required=True, help="合并后文件的输出文件夹路径")
    args = parser.parse_args()

    print(f"目标文件夹: {args.target}")
    print(f"输出文件夹: {args.output}")

    merge_excels(args.target, args.output)

    print("\n脚本执行完毕。")
