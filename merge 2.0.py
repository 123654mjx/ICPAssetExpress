import os
import pandas as pd
import argparse


def merge_excels(target_folder, output_folder):
    # url_fingerprint 相关的 DataFrame 列表
    initial_frames = []
    valid_frames = []
    invalid_frames = []

    # fscan_result 相关的数据，按 sheet 名称存储 DataFrame 列表
    fscan_data_by_sheet = {}

    # quake_result 相关的 DataFrame 列表
    quake_result_frames = []

    # fofa_results 相关的 DataFrame 列表
    fofa_results_frames = []

    for root, dirs, files in os.walk(target_folder):
        for file in files:
            if file.endswith(".xlsx"):
                file_path = os.path.join(root, file)
                print(f"正在处理文件: {file_path}")

                excel_file_handler = None  # 初始化 handler
                sheet_names_list = []

                try:
                    excel_file_handler = pd.ExcelFile(file_path, engine="openpyxl")
                    sheet_names_list = excel_file_handler.sheet_names
                except Exception as e:
                    # 对于 quake 和 fofa 文件，即使读取 sheets 失败也可能只读取第一个 sheet
                    if file.startswith("quake_result_") or file.startswith("fofa_results_"):
                        pass
                    else:
                        print(
                            f"警告: 读取文件 sheets 失败: {file_path}, 错误信息: {e}. 跳过此文件对于 url_fingerprint 和 fscan 的处理。")
                        continue

                # 处理 url_fingerprint 表
                if file.startswith("url_fingerprint_"):
                    if not excel_file_handler or not sheet_names_list:
                        print(f"警告: url_fingerprint_ 文件 {file_path} 无法获取 sheet 列表，跳过。")
                        if excel_file_handler: excel_file_handler.close()  # 确保关闭
                        continue
                    for sheet_name in sheet_names_list:
                        try:
                            df = pd.read_excel(excel_file_handler, sheet_name=sheet_name, engine="openpyxl")
                            df["来源表名"] = file
                            if "有效" in sheet_name:
                                valid_frames.append(df)
                            elif "无效" in sheet_name:
                                invalid_frames.append(df)
                            else:
                                initial_frames.append(df)
                        except Exception as e:
                            print(f"读取 url_fingerprint sheet 失败: {file_path} - {sheet_name}, 错误信息: {e}")

                # 处理 fscan_result 表
                elif file.startswith("fscan_result_"):
                    if not excel_file_handler or not sheet_names_list:
                        print(f"警告: fscan_result_ 文件 {file_path} 无法获取 sheet 列表，跳过。")
                        if excel_file_handler: excel_file_handler.close()  # 确保关闭
                        continue
                    for sheet_name in sheet_names_list:
                        try:
                            df = pd.read_excel(excel_file_handler, sheet_name=sheet_name, engine="openpyxl")
                            df["来源表名"] = file
                            if sheet_name not in fscan_data_by_sheet:
                                fscan_data_by_sheet[sheet_name] = []
                            fscan_data_by_sheet[sheet_name].append(df)
                        except Exception as e:
                            print(f"读取 fscan_result sheet 失败: {file_path} - {sheet_name}, 错误信息: {e}")

                # quake 合并逻辑
                elif file.startswith("quake_result_"):
                    try:
                        df = pd.read_excel(file_path, engine="openpyxl")
                        quake_result_frames.append(df)
                    except Exception as e:
                        print(f"读取 quake 文件失败: {file_path}, 错误信息: {e}")

                # fofa 合并逻辑 (修改：添加来源表名)
                elif file.startswith("fofa_results_"):
                    try:
                        df = pd.read_excel(file_path, engine="openpyxl")
                        df["来源表名"] = file  # 添加来源表名列
                        fofa_results_frames.append(df)
                    except Exception as e:
                        print(f"读取 fofa 文件失败: {file_path}, 错误信息: {e}")

                if excel_file_handler:
                    excel_file_handler.close()

    os.makedirs(output_folder, exist_ok=True)
    print(f"\n开始写入合并后的文件到目录: {output_folder}")

    if initial_frames or valid_frames or invalid_frames:
        output_path_url = os.path.join(output_folder, "url_fingerprint_all.xlsx")
        try:
            with pd.ExcelWriter(output_path_url, engine="openpyxl") as writer:
                if initial_frames:
                    pd.concat(initial_frames, ignore_index=True).to_excel(writer, sheet_name="原始表", index=False)
                    print(f"已写入 sheet '原始表' 到 {output_path_url}")
                if valid_frames:
                    pd.concat(valid_frames, ignore_index=True).to_excel(writer, sheet_name="有效表", index=False)
                    print(f"已写入 sheet '有效表' 到 {output_path_url}")
                if invalid_frames:
                    pd.concat(invalid_frames, ignore_index=True).to_excel(writer, sheet_name="无效表", index=False)
                    print(f"已写入 sheet '无效表' 到 {output_path_url}")
            print(f"文件 url_fingerprint_all.xlsx 已成功保存。")
        except Exception as e:
            print(f"写入 url_fingerprint_all.xlsx 文件失败: {e}")

    if fscan_data_by_sheet:
        output_path_fscan = os.path.join(output_folder, "fscan_result_all.xlsx")
        try:
            with pd.ExcelWriter(output_path_fscan, engine="openpyxl") as writer:
                for sheet_name, frames_list in fscan_data_by_sheet.items():
                    if frames_list:
                        combined_df = pd.concat(frames_list, ignore_index=True)
                        combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"已写入 sheet '{sheet_name}' 到 {output_path_fscan}")
                    else:
                        print(f"fscan_result_all.xlsx 中 sheet '{sheet_name}' 无数据可写。")
            print(f"文件 fscan_result_all.xlsx 已成功保存。")
        except Exception as e:
            print(f"写入 fscan_result_all.xlsx 文件失败: {e}")

    if quake_result_frames:
        output_path_quake = os.path.join(output_folder, "quake_result_all.xlsx")
        try:
            pd.concat(quake_result_frames, ignore_index=True).to_excel(output_path_quake, index=False)
            print(f"文件 quake_result_all.xlsx 已成功保存。")
        except Exception as e:
            print(f"写入 quake_result_all.xlsx 文件失败: {e}")

    # fofa 合并逻辑 (修改：添加来源表名说明)
    if fofa_results_frames:
        output_path_fofa = os.path.join(output_folder, "fofa_results_all.xlsx")
        try:
            pd.concat(fofa_results_frames, ignore_index=True).to_excel(output_path_fofa, index=False)
            print(f"文件 fofa_results_all.xlsx 已成功保存。")
        except Exception as e:
            print(f"写入 fofa_results_all.xlsx 文件失败: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""合并指定文件夹下的各类Excel文件。

详细说明:
  - 脚本会遍历目标文件夹及其子文件夹。
  - 自动识别并处理以下四种类型（基于文件名前缀）的 .xlsx 文件:
    * 'url_fingerprint_*'
    * 'fscan_result_*'
    * 'quake_result_*'
    * 'fofa_results_*'

输出文件规则:
  1. url_fingerprint_all.xlsx:
     - 根据源文件内sheet名是否包含"有效"或"无效"进行分类。
     - 合并到 '有效表', '无效表', '原始表' 三个sheet中。
     - 所有sheet均会添加 '来源表名' 列。

  2. fscan_result_all.xlsx:
     - 保留源文件中的原始sheet名称。
     - 不同文件中的同名sheet数据将合并到以此命名的sheet中。
     - 所有sheet均会添加 '来源表名' 列。

  3. quake_result_all.xlsx:
     - 直接合并所有匹配文件的第一个sheet数据。
     - 不添加 '来源表名' 列。

  4. fofa_results_all.xlsx:
     - 直接合并所有匹配文件的第一个sheet数据。
     - 会添加 '来源表名' 列。 # 修改：明确说明会添加来源表名
""",
        formatter_class=argparse.RawTextHelpFormatter  # 保持原始文本格式
    )
    parser.add_argument("-t", "--target", required=True, help="包含Excel文件的目标文件夹路径")
    parser.add_argument("-o", "--output", required=True, help="合并后文件的输出文件夹路径")
    args = parser.parse_args()

    print(f"目标文件夹: {args.target}")
    print(f"输出文件夹: {args.output}")

    merge_excels(args.target, args.output)

    print("\n脚本执行完毕。")