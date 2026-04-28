"""
测试解析 SQL 文件（支持单文件和目录两种模式）

使用方法：
1. 修改 FILE_PATH 为要测试的文件或目录路径
2. 设置 IS_DIRECTORY = True/False 来切换模式
3. 运行此脚本
"""

import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import orchestrationService
from apps.extend.utils.utils import DBUtils
import time


if __name__ == "__main__":
    # ==================== 配置区域 ====================
    # 测试单个文件
    # FILE_PATHS = [r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dwd\dwd_aibreeding_collect_semen_plan.sql"]
    # dws_fpf_porker_forage_record_group

    FILE_PATHS = [
        r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dwd",
        r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dws",
        r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\ads"]

    for FILE_PATH in FILE_PATHS:
        print(f"📄 路径: {FILE_PATH}\n")
        try:
            session = DBUtils.create_local_session()
            # 创建服务实例
            platform_service = orchestrationService(session)

            # 调用 Service 层的处理方法（自动判断文件/目录，自动识别层级类型）
            start_time = time.time()
            result = platform_service.process(
                input_path=FILE_PATH
            )
            processing_duration = time.time() - start_time

            # 打印结果
            print("\n" + "=" * 80)
            print("📊 测试结果汇总")
            print("=" * 80)
            print(f"⏱️  总耗时: {processing_duration:.2f}秒")
            print(f"📁 文件总数: {result.get('total_count', 0)}")
            print(f"✅ 成功数量: {result.get('success_count', 0)}")
            print(f"❌ 失败数量: {result.get('failed_count', 0)}")

            # 显示成功文件列表
            success_files = result.get('success_files', [])
            if success_files:
                print(f"\n✅ 成功文件 ({len(success_files)}个):")
                for i, file_name in enumerate(success_files, 1):
                    print(f"   {i}. {file_name}")

            # 显示失败文件列表
            failed_files = result.get('failed_files', [])
            if failed_files:
                print(f"\n❌ 失败文件 ({len(failed_files)}个):")
                for i, file_name in enumerate(failed_files, 1):
                    print(f"   {i}. {file_name}")

            # 显示详细结果（可选）
            print("\n" + "=" * 80)
            print("📋 详细结果（JSON格式）")
            print("=" * 80)
            import json
            # 只打印摘要信息，不打印完整的 results 数组
            summary_result = {
                'success': result.get('success'),
                'total_count': result.get('total_count'),
                'success_count': result.get('success_count'),
                'failed_count': result.get('failed_count'),
                'message': result.get('message')
            }
            print(json.dumps(summary_result, indent=2, ensure_ascii=False))

        except Exception as e:
            print(f"\n❌ 测试异常：{str(e)}")
        finally:
            session.close()
