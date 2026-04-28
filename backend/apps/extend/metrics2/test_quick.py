"""
快速测试单个 SQL 文件
"""
import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import orchestrationService
from apps.extend.utils.utils import DBUtils

if __name__ == "__main__":
    sql_file = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dws\dws_fpf_porker_forage_record_group.sql"
    
    print(f"🧪 测试文件: {sql_file}\n")
    
    session = DBUtils.create_local_session()
    try:
        platform_service = orchestrationService(session)
        result = platform_service.process(
            input_path=sql_file
        )
        
        if result.get('success'):
            print("\n✅ 测试成功！")
        else:
            print("\n❌ 测试失败！")
            print(f"错误信息：{result.get('message', '未知错误')}")
    finally:
        session.close()
