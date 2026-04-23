"""
单独测试单个 SQL 文件的解析
"""

import sys
import os
import logging

# 配置日志 - 显示详细信息用于调试
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)

# 设置特定logger的级别
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("MetricsPlatformService").setLevel(logging.INFO)
logging.getLogger("LineageService").setLevel(logging.DEBUG)  # ⚠️ 开启DEBUG查看详细错误
logging.getLogger("MetricsService").setLevel(logging.INFO)
logging.getLogger("DimService").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)  # ⚠️ 开启SQL日志查看具体SQL
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import MetricsPlatformService
from apps.extend.utils.utils import DBUtils
import traceback
import time
import re


def detect_group_by_in_sql(sql_content: str) -> bool:
    """
    检测SQL中是否包含GROUP BY子句
    
    Args:
        sql_content: SQL内容
        
    Returns:
        True 如果包含GROUP BY
    """
    # 使用正则表达式检测GROUP BY（忽略大小写）
    pattern = r'\bGROUP\s+BY\b'
    return bool(re.search(pattern, sql_content, re.IGNORECASE))


def determine_actual_layer(sql_file_path: str, detected_layer: str) -> str:
    """
    根据SQL内容确定实际应该使用的处理流程
    
    Args:
        sql_file_path: SQL文件路径
        detected_layer: 从路径检测到的层级（DIM/DWD/DWS/ADS/AUTO）
        
    Returns:
        实际应该使用的处理流程（DIM/DWD/METRIC）
    """
    try:
        # 读取SQL文件内容
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 如果是AUTO，先从路径推断层级
        actual_detected = detected_layer
        if detected_layer == "AUTO":
            # 从文件路径推断层级
            path_lower = sql_file_path.lower()
            if '\\dwd\\' in path_lower or '/dwd/' in path_lower:
                actual_detected = "DWD"
            elif '\\dim\\' in path_lower or '/dim/' in path_lower:
                actual_detected = "DIM"
            elif '\\dws\\' in path_lower or '/dws/' in path_lower:
                actual_detected = "DWS"
            elif '\\ads\\' in path_lower or '/ads/' in path_lower:
                actual_detected = "ADS"
        
        # 如果是DWD层，检查是否有GROUP BY
        if actual_detected == "DWD":
            if detect_group_by_in_sql(sql_content):
                print(f"⚠️  DWD层SQL检测到GROUP BY，切换到METRIC流程处理")
                return "METRIC"
    except Exception as e:
        print(f"⚠️  读取SQL文件失败: {e}，使用原始层级")
    
    return detected_layer


def validate_parse_completeness(result: dict, layer_type: str) -> tuple:
    """
    验证解析结果的完整性
    
    Args:
        result: 解析结果
        layer_type: 层级类型
        
    Returns:
        (is_complete: bool, issues: list) - 是否完整和问题列表
    """
    issues = []
    
    if not result.get('success'):
        return False, ["解析本身失败"]
    
    parsed_results = result.get('parsed_results', [])
    if not parsed_results:
        return False, ["没有解析结果"]
    
    first_result = parsed_results[0]
    parsed_data = first_result.get('parsed_data', {})
    
    # 根据不同层级设置不同的期望
    if layer_type == "WIDE":
        # WIDE层（宽表/明细层）应该有字段血缘
        execution_result = result.get('execution_result', {})
        table_stats = execution_result.get('table_stats', {})
        field_lineage_count = table_stats.get('field_lineage', 0)
        
        if field_lineage_count == 0:
            issues.append("WIDE层未提取到任何字段血缘")
        
        # 检查是否有基本的表信息
        basic_info = parsed_data.get('basic_info', {})
        if not basic_info.get('target_table'):
            issues.append("未识别到目标表")
    
    elif layer_type == "METRIC" or layer_type == "ADS":
        # ADS/METRIC层应该有指标定义
        metric_definitions = parsed_data.get('metric_definitions', [])
        if len(metric_definitions) == 0:
            issues.append("指标层未生成任何指标定义")
    
    elif layer_type == "DIM":
        # DIM层应该有维度定义
        dim_definitions = parsed_data.get('dim_definitions', [])
        if len(dim_definitions) == 0:
            issues.append("DIM层未生成任何维度定义")
    
    is_complete = len(issues) == 0
    return is_complete, issues


def test_single_file():
    """测试单个SQL文件"""
    
    sql_file_path = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dws\dws_female_parity_weaning_detail.sql"
    
    print("=" * 80)
    print(f"🔍 测试文件：{os.path.basename(sql_file_path)}")
    print("=" * 80)
    print(f"📄 完整路径：{sql_file_path}")
    print("=" * 80)
    
    # 检查文件是否存在
    if not os.path.exists(sql_file_path):
        print(f"\n❌ 文件不存在：{sql_file_path}")
        return
    
    # 获取数据库会话
    session = DBUtils.create_local_session()
    
    try:
        # 创建服务实例
        platform_service = MetricsPlatformService(session)
        
        # ⚠️ 步骤1：使用AUTO模式，让主程序自动识别和处理
        print(f"\n🔍 步骤1: 使用AUTO模式...")
        actual_layer = "AUTO"
        print(f"   - 使用层级: {actual_layer} (主程序将自动识别)")
        
        # 调用解析方法
        print(f"\n🔄 步骤2: 开始解析...")
        start_time = time.time()
        
        result = platform_service.process_metrics_from_sql(
            input_path=sql_file_path,
            is_directory=False,
            layer_type="AUTO"  # 使用AUTO模式，让主程序自动处理
        )
        
        processing_duration = time.time() - start_time
        
        # ⚠️ 步骤3：验证解析完整性
        print(f"\n🔍 步骤3: 验证解析完整性...")
        is_complete, issues = validate_parse_completeness(result, actual_layer)
        
        if not is_complete:
            print(f"⚠️  解析不完整，发现以下问题:")
            for issue in issues:
                print(f"   ❌ {issue}")
        else:
            print(f"✅ 解析完整性检查通过")
        
        # 打印结果
        print("\n" + "=" * 80)
        print("📊 解析结果")
        print("=" * 80)
        print(f"⏱️  耗时: {processing_duration:.2f}秒")
        print(f"📋 处理流程: {actual_layer}")
        print(f"✅ 完整性: {'通过' if is_complete else '不通过'}")
        
        if result.get('success'):
            print(f"✅ 解析成功！")
            print(f"\n返回数据结构:")
            print(f"  - success: {result.get('success')}")
            print(f"  - message: {result.get('message', 'N/A')}")
            
            parsed_results = result.get('parsed_results', [])
            print(f"  - parsed_results 数量: {len(parsed_results)}")
            
            if parsed_results:
                for idx, parsed_result in enumerate(parsed_results, 1):
                    print(f"\n  📋 解析结果 {idx}:")
                    parsed_data = parsed_result.get('parsed_data', {})
                    basic_info = parsed_data.get('basic_info', {})
                    print(f"     - 目标表: {basic_info.get('target_table', 'N/A')}")
                    print(f"     - 仓库层级: {basic_info.get('warehouse_layer', 'N/A')}")
                    
                    # ⚠️ 打印AI解析的完整basic_info
                    print(f"\n     🔍 AI解析的basic_info:")
                    for key, value in basic_info.items():
                        if key not in ['sql_content']:  # 排除SQL内容
                            print(f"        - {key}: {value}")
                    
                    # 打印指标定义
                    metric_definitions = parsed_data.get('metric_definitions', [])
                    print(f"\n     - 指标定义数量: {len(metric_definitions)}")
                    if metric_definitions:
                        for m in metric_definitions[:3]:  # 只显示前3个
                            print(f"        * {m.get('metric_name', 'N/A')} ({m.get('metric_code', 'N/A')})")
                    
                    # 打印维度定义
                    dim_definitions = parsed_data.get('dim_definitions', [])
                    print(f"     - 维度定义数量: {len(dim_definitions)}")
                    if dim_definitions:
                        for d in dim_definitions[:3]:  # 只显示前3个
                            print(f"        * {d.get('dim_name', 'N/A')} ({d.get('dim_code', 'N/A')})")
                    
                    # ⚠️ 打印field_lineage中的target_field_mark统计
                    field_lineage = parsed_data.get('field_lineage', [])
                    if field_lineage:
                        mark_stats = {}
                        for fl in field_lineage:
                            mark = fl.get('target_field_mark', 'normal')
                            mark_stats[mark] = mark_stats.get(mark, 0) + 1
                        print(f"\n     - 字段血缘标记统计: {mark_stats}")
                    
                    execution_result = result.get('execution_result', {})
                    table_stats = execution_result.get('table_stats', {})
                    if table_stats:
                        print(f"     - 表统计: {table_stats}")
        else:
            print(f"❌ 解析失败！")
            print(f"\n错误信息：")
            print(f"  - success: {result.get('success')}")
            print(f"  - message: {result.get('message', '未知错误')}")
            
            # 如果有更多错误详情，打印出来
            if 'error_details' in result:
                print(f"\n详细错误：")
                print(result['error_details'])
    
    except Exception as e:
        print(f"\n❌ 解析异常！")
        print(f"\n异常类型: {type(e).__name__}")
        print(f"异常信息: {str(e)}")
        print(f"\n完整堆栈跟踪:")
        traceback.print_exc()
    
    finally:
        session.close()
        print("\n" + "=" * 80)


if __name__ == "__main__":
    test_single_file()
