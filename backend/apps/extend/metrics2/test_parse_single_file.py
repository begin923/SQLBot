"""
测试解析各层 SQL 文件（DIM、DWD、DWS、ADS）

使用方法：
1. 配置各层的SQL文件路径
2. 运行此脚本，会按顺序测试各层
"""

import sys
import os
import logging

# 配置日志（避免重复输出）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 强制重新配置，避免重复handler
)

# 设置特定logger的级别，减少冗余日志
logging.getLogger("httpx").setLevel(logging.WARNING)  # HTTP请求日志
logging.getLogger("MetricsPlatformService").setLevel(logging.INFO)
logging.getLogger("LineageService").setLevel(logging.INFO)
logging.getLogger("MetricsService").setLevel(logging.INFO)
logging.getLogger("DimService").setLevel(logging.INFO)

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import MetricsPlatformService
from apps.extend.utils.utils import DBUtils



def test_layer(sql_file_path: str, layer_type: str = "AUTO", session=None):
    """
    测试单个层级的 SQL 文件解析
    
    Args:
        sql_file_path: SQL 文件路径
        layer_type: 层级类型（AUTO/DIM/DWD/METRIC），默认 AUTO 自动识别
        session: 数据库会话
    """
    print("\n" + "=" * 80)
    print(f"🔍 测试 {layer_type} 层")
    print("=" * 80)
    print(f"📄 文件路径：{sql_file_path}")
    print("=" * 80)
    
    # 检查文件是否存在
    if not os.path.exists(sql_file_path):
        print(f"\n❌ 文件不存在：{sql_file_path}")
        return False
    
    try:
        # 创建服务实例
        platform_service = MetricsPlatformService(session)
        
        # 调用解析方法
        print(f"\n🔄 开始解析...")
        result = platform_service.process_metrics_from_sql(
            input_path=sql_file_path,
            is_directory=False,
            layer_type=layer_type
        )
        
        # 打印结果
        if result.get('success'):
            print(f"✅ {layer_type} 层解析成功！")
            
            # ⚠️ 如果是 AUTO 模式，显示实际识别的层级类型
            if layer_type == "AUTO":
                parsed_results = result.get('parsed_results', [])
                if parsed_results:
                    first_result = parsed_results[0]
                    parsed_data = first_result.get('parsed_data', {})
                    basic_info = parsed_data.get('basic_info', {})
                    actual_layer = basic_info.get('warehouse_layer', 'UNKNOWN').upper()
                    print(f"🎯 自动识别为: {actual_layer} 层")
            
            # 从 parsed_results 中获取详细信息
            parsed_results = result.get('parsed_results', [])
            if parsed_results:
                first_result = parsed_results[0]
                parsed_data = first_result.get('parsed_data', {})
                basic_info = parsed_data.get('basic_info', {})
                
                print(f"\n📊 统计信息：")
                print(f"   - 目标表：{basic_info.get('target_table', 'N/A')}")
                print(f"   - 表描述：{basic_info.get('table_desc', 'N/A')}")
                
                # ⚠️ 调试：打印 AI 返回的血缘数据量
                table_lineage_raw = parsed_data.get('table_lineage', [])
                field_lineage_raw = parsed_data.get('field_lineage', [])
                print(f"\n🔍 AI 原始解析结果：")
                print(f"   - table_lineage: {len(table_lineage_raw)} 条")
                print(f"   - field_lineage: {len(field_lineage_raw)} 条")
                if table_lineage_raw:
                    first_tl = table_lineage_raw[0]
                    print(f"   - 示例: source_table={first_tl.get('source_table')}, target_table={first_tl.get('target_table')}")
                if field_lineage_raw:
                    first_fl = field_lineage_raw[0]
                    print(f"   - 示例: source_field={first_fl.get('source_field')}, target_field={first_fl.get('target_field')}")
                
                # 根据层级类型显示不同的统计信息
                # ⚠️ 注意：AUTO 模式下，需要从 basic_info 中获取实际的层级类型
                actual_layer = basic_info.get('warehouse_layer', layer_type).upper()
                
                if actual_layer == "DWD":
                    # DWD 层：显示字段数量和血缘信息
                    fields = parsed_data.get('fields', [])
                    table_lineage = parsed_data.get('table_lineage', [])
                    field_lineage = parsed_data.get('field_lineage', [])
                    
                    print(f"   - 字段数：{len(fields)}")
                    print(f"   - 表血缘数：{len(table_lineage)}")
                    print(f"   - 字段血缘数：{len(field_lineage)}")
                    
                    # 显示字段列表（前5个）
                    if fields:
                        print(f"\n📋 识别的字段（前5个）：")
                        for i, field in enumerate(fields[:5], 1):
                            print(f"   {i}. {field.get('field_name', 'N/A')} ({field.get('field_en', 'N/A')}) - {field.get('field_type', 'N/A')}")
                        if len(fields) > 5:
                            print(f"   ... 还有 {len(fields) - 5} 个字段")
                    
                    # 显示表血缘（前3个）
                    if table_lineage:
                        print(f"\n🔗 表血缘关系（前3个）：")
                        for i, tl in enumerate(table_lineage[:3], 1):
                            source_name = tl.get('source_table_name', '')
                            target_name = tl.get('target_table_name', '')
                            name_info = f" ({source_name} -> {target_name})" if source_name and target_name else ""
                            print(f"   {i}. {tl.get('source_table', 'N/A')} -> {tl.get('target_table', 'N/A')}{name_info}")
                        if len(table_lineage) > 3:
                            print(f"   ... 还有 {len(table_lineage) - 3} 条表血缘")
                    
                    # ⚠️ 显示字段血缘示例（前3个）
                    if field_lineage:
                        print(f"\n📊 字段血缘示例（前3个）：")
                        for i, fl in enumerate(field_lineage[:3], 1):
                            source_field_name = fl.get('source_field_name', '')
                            target_field_name = fl.get('target_field_name', '')
                            field_mark = fl.get('target_field_mark', 'normal')
                            print(f"   {i}. [{field_mark}] {fl.get('source_field', 'N/A')}")
                            if source_field_name:
                                print(f"      → {fl.get('target_field', 'N/A')} ({target_field_name})")
                            else:
                                print(f"      → {fl.get('target_field', 'N/A')}")
                        if len(field_lineage) > 3:
                            print(f"   ... 还有 {len(field_lineage) - 3} 条字段血缘")
                            
                elif actual_layer == "DIM":
                    # DIM 层：显示字段数量
                    fields = parsed_data.get('fields', [])
                    print(f"   - 字段数：{len(fields)}")
                    
                    # 显示字段列表（前5个）
                    if fields:
                        print(f"\n📋 维度字段（前5个）：")
                        for i, field in enumerate(fields[:5], 1):
                            print(f"   {i}. {field.get('field_name', 'N/A')} ({field.get('field_en', 'N/A')}) - {field.get('dim_type', 'N/A')}")
                        if len(fields) > 5:
                            print(f"   ... 还有 {len(fields) - 5} 个字段")
                            
                else:  # METRIC (DWS/ADS)
                    # METRIC 层：显示字段数量
                    fields = parsed_data.get('fields', [])
                    print(f"   - 字段数：{len(fields)}")
                    
                    # 显示字段列表（前5个）
                    if fields:
                        print(f"\n📋 指标/维度字段（前5个）：")
                        for i, field in enumerate(fields[:5], 1):
                            field_type = field.get('field_type', 'N/A')
                            print(f"   {i}. {field.get('field_name', 'N/A')} ({field.get('field_en', 'N/A')}) - {field_type}")
                        if len(fields) > 5:
                            print(f"   ... 还有 {len(fields) - 5} 个字段")
                
                # ⚠️ 显示各表的数据量统计
                execution_result = result.get('execution_result', {})
                table_stats = execution_result.get('table_stats', {})
                if table_stats:
                    print(f"\n💾 数据写入统计：")
                    for table_name, count in sorted(table_stats.items()):
                        # 根据表名添加图标
                        icon_map = {
                            'dim_definition': '📏',
                            'dim_field_mapping': '🔗',
                            'table_lineage': '🔗',
                            'field_lineage': '📊',
                            'metric_definition': '📈',
                            'metric_source_mapping': '🎯',
                            'metric_dim_rel': '🔀',
                            'metric_compound_rel': '🧩',
                            'metric_lineage': '🌳'
                        }
                        icon = icon_map.get(table_name, '📄')
                        print(f"   {icon} {table_name}: {count} 条")
            
            return True
            
        else:
            print(f"❌ {layer_type} 层解析失败！")
            error_msg = result.get('message', '未知错误')
            print(f"错误信息：{error_msg}")
            
            # ⚠️ 特殊处理：SELECT * 需要改进 SQL
            if result.get('needs_sql_improvement'):
                matched_pattern = result.get('matched_pattern', '未知通配符')
                print("\n" + "=" * 80)
                print("💡 改进建议")
                print("=" * 80)
                print(f"检测到 SQL 中存在通配符：**{matched_pattern}**")
                print("\n这会导致字段解析不准确，AI 无法知道通配符展开后的具体字段。")
                print("\n请修改 SQL，将所有通配符展开为明确的字段列表。")
                print("=" * 80)
            
            return False
    
    except Exception as e:
        print(f"\n❌ {layer_type} 层解析异常：{str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_all_layers():
    """按顺序测试所有层级（DIM、DWD、DWS、ADS）"""
    
    # ==================== 配置区域 ====================
    # 配置各层的SQL文件路径（每层只需要1个文件）
    # 注意：使用 AUTO 模式，系统会根据文件路径自动识别层级类型
    # - DIM 路径包含 /dim/ → 自动识别为 DIM 层
    # - DWD 路径包含 /dwd/ → 自动识别为 DWD 层
    # - DWS/ADS 路径 → 自动识别为 METRIC 层
    test_files = [
        # r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dim\dim_hmc_plus_org_farm_entity.sql"
        # r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dwd\dwd_aib_female_mating_detail_new.sql"
        # r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\dws\dws_inb_pig_semen_product_breed_loc_day.sql"
        # r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour\ads\ads_anc_female_backfat_month_new2_data.sql"
    ]
    
    # ==================== 执行测试 ====================
    print("🚀 开始测试各层 SQL 文件解析")
    print("=" * 80)
    
    # 获取数据库会话
    session = DBUtils.create_local_session()
    
    results = {}
    
    try:
        # 按顺序测试各层（使用 AUTO 模式自动识别）
        for idx, file_path in enumerate(test_files, 1):
            print(f"\n{'='*80}")
            print(f"📋 测试文件 {idx}/{len(test_files)}")
            print(f"{'='*80}")
            
            success = test_layer(file_path, layer_type="AUTO", session=session)
            results[file_path] = success
            
            # 每个文件测试后暂停一下，方便查看结果
            if idx < len(test_files):
                print("\n⏸️  按回车继续测试下一个文件...")
                input()
        
        # 打印总结
        print("\n" + "=" * 80)
        print("📊 测试总结")
        print("=" * 80)
        for file_path, success in results.items():
            file_name = os.path.basename(file_path)
            status = "✅ 成功" if success else "❌ 失败"
            print(f"   {file_name}: {status}")
        
        all_success = all(results.values())
        if all_success:
            print("\n🎉 所有文件测试通过！")
        else:
            failed_files = [os.path.basename(path) for path, success in results.items() if not success]
            print(f"\n⚠️  以下文件测试失败: {', '.join(failed_files)}")
        
    finally:
        session.close()
        print("\n" + "=" * 80)


if __name__ == "__main__":
    test_all_layers()
