"""
测试解析 SQL 文件（支持单文件和目录两种模式）

使用方法：
1. 修改 FILE_PATH 为要测试的文件或目录路径
2. 设置 IS_DIRECTORY = True/False 来切换模式
3. 运行此脚本
"""

import logging
import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import MetricsPlatformService
from apps.extend.utils.utils import DBUtils
import time


if __name__ == "__main__":
    # ==================== 配置区域 ====================
    # 测试单个文件
    FILE_PATH = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dim"
    
    # 测试整个目录（取消注释使用）
    # FILE_PATH = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dws"
    # ================================================
    
    print(f"📄 路径: {FILE_PATH}\n")
    
    session = DBUtils.create_local_session()
    try:
        # 创建服务实例
        platform_service = MetricsPlatformService(session)
        
        # 调用 Service 层的处理方法（自动判断文件/目录，自动识别层级类型）
        start_time = time.time()
        result = platform_service.process_sql_files(
            input_path=FILE_PATH
        )
        processing_duration = time.time() - start_time
        
        # 打印结果
        print("\n" + "=" * 80)
        print("📊 测试总结")
        print("=" * 80)
        print(f"⏱️  总耗时: {processing_duration:.2f}秒")
        
        if result.get('success'):
            file_result = result.get('file_result', {})
            total_files = file_result.get('total_files', 0)
            processed_files = file_result.get('processed_files', 0)
            failed_files = file_result.get('failed_files', 0)
            
            print(f"   - 总文件数: {total_files}")
            print(f"   - 成功: {processed_files}")
            print(f"   - 失败: {failed_files}")
            
            if failed_files > 0:
                print(f"\n⚠️  以下文件测试失败:")
                for file_data in file_result.get('results', []):
                    if not file_data.get('success'):
                        file_name = os.path.basename(file_data.get('file_path', ''))
                        print(f"   ❌ {file_name}: {file_data.get('message', '未知错误')}")
            else:
                print("\n🎉 所有文件测试通过！")
        else:
            print(f"❌ 测试失败：{result.get('message', '未知错误')}")
    
    except Exception as e:
        print(f"\n❌ 测试异常：{str(e)}")
    finally:
        session.close()
