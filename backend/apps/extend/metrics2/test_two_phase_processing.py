"""
测试两阶段指标平台建设流程

阶段1: 解析 DIM 层 ETL 脚本，建立维度字典
阶段2: 解析 DWD/DWS/ADS 层 ETL 脚本，提取指标并引用已有维度

使用方法：
1. 准备 DIM 层和 METRIC 层的 SQL 文件路径
2. 运行此脚本观察两阶段处理流程
"""

import sys
import os
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import MetricsPlatformService
from apps.extend.utils.utils import DBUtils


def test_two_phase_processing():
    """测试两阶段处理流程"""
    
    print("=" * 80)
    print("测试两阶段指标平台建设流程")
    print("=" * 80)
    
    # ==================== 配置区域 ====================
    # DIM 层路径（维度定义）
    dim_layer_path = "D:/codes/yingzi-data-datawarehouse-release/source/sql/doris/fpf/hour/dim"
    
    # METRIC 层路径（dwd/dws/ads，包含指标）
    metric_layer_path = "D:/codes/yingzi-data-datawarehouse-release/source/sql/doris/fpf/hour/dwd"
    
    # 如果目录不存在，使用示例说明
    if not os.path.exists(dim_layer_path):
        print(f"\n⚠️  DIM 层目录不存在：{dim_layer_path}")
        print("请修改脚本中的路径配置\n")
        return
    
    if not os.path.exists(metric_layer_path):
        print(f"\n⚠️  METRIC 层目录不存在：{metric_layer_path}")
        print("请修改脚本中的路径配置\n")
        return
    
    # ==================== 执行测试 ====================
    session = DBUtils.create_local_session()
    
    try:
        platform_service = MetricsPlatformService(session)
        
        # ========== 阶段1: 处理 DIM 层 ==========
        print("\n" + "=" * 80)
        print("📦 阶段1: 处理 DIM 层（建立维度字典）")
        print("=" * 80)
        print(f"📂 目录路径：{dim_layer_path}\n")
        
        dim_result = platform_service.process_metrics_from_sql(
            input_path=dim_layer_path,
            is_directory=True,
            layer_type="DIM"  # 明确指定为 DIM 层
        )
        
        if dim_result.get('success'):
            print(f"✅ DIM 层处理成功！")
            execution_result = dim_result.get('execution_result', {})
            print(f"   - 执行 SQL 数：{execution_result.get('executed_count', 0)}")
            print(f"   - 失败 SQL 数：{execution_result.get('failed_count', 0)}")
            
            # 显示生成的 INSERT SQL 统计
            insert_sqls = dim_result.get('insert_sqls', [])
            if insert_sqls:
                print(f"\n💾 生成的批量 INSERT SQL：")
                for sql in insert_sqls[:5]:  # 只显示前5条
                    # 提取表名
                    if 'INSERT INTO' in sql:
                        table_name = sql.split('INSERT INTO')[1].split()[0]
                        # 提取记录数
                        if '(' in sql and '条记录' in sql:
                            count = sql.split('(')[1].split(' 条记录')[0]
                            print(f"   - {table_name}: {count} 条")
        else:
            print(f"❌ DIM 层处理失败：{dim_result.get('message')}")
            return
        
        # ========== 阶段2: 处理 METRIC 层 ==========
        print("\n" + "=" * 80)
        print("📊 阶段2: 处理 METRIC 层（提取指标并引用维度）")
        print("=" * 80)
        print(f"📂 目录路径：{metric_layer_path}\n")
        
        metric_result = platform_service.process_metrics_from_sql(
            input_path=metric_layer_path,
            is_directory=True,
            layer_type="METRIC"  # 明确指定为 METRIC 层
        )
        
        if metric_result.get('success'):
            print(f"✅ METRIC 层处理成功！")
            execution_result = metric_result.get('execution_result', {})
            print(f"   - 执行 SQL 数：{execution_result.get('executed_count', 0)}")
            print(f"   - 失败 SQL 数：{execution_result.get('failed_count', 0)}")
            
            # 显示生成的 INSERT SQL 统计
            insert_sqls = metric_result.get('insert_sqls', [])
            if insert_sqls:
                print(f"\n💾 生成的批量 INSERT SQL：")
                for sql in insert_sqls[:10]:  # 只显示前10条
                    # 提取表名
                    if 'INSERT INTO' in sql:
                        table_name = sql.split('INSERT INTO')[1].split()[0]
                        # 提取记录数
                        if '(' in sql and '条记录' in sql:
                            count = sql.split('(')[1].split(' 条记录')[0]
                            print(f"   - {table_name}: {count} 条")
            
            # 显示处理的指标详情
            processed_results = metric_result.get('processed_results', [])
            if processed_results:
                print(f"\n🎯 处理的指标详情：")
                total_metrics = 0
                total_dims = 0
                for result in processed_results:
                    if result.get('success'):
                        processed_data = result.get('processed_data', {})
                        metrics = processed_data.get('metrics', [])
                        dimensions = processed_data.get('dimensions', [])
                        total_metrics += len(metrics)
                        total_dims += len(dimensions)
                        
                        file_path = result.get('file_path', '')
                        file_name = os.path.basename(file_path)
                        print(f"   📄 {file_name}: {len(metrics)} 个指标, {len(dimensions)} 个维度")
                
                print(f"\n📊 总计：{total_metrics} 个指标, {total_dims} 个维度引用")
        else:
            print(f"❌ METRIC 层处理失败：{metric_result.get('message')}")
        
        # ========== 总结 ==========
        print("\n" + "=" * 80)
        print("✅ 两阶段处理完成！")
        print("=" * 80)
        print("\n📝 处理流程总结：")
        print("   1. DIM 层 → 建立维度定义 (dim_definition)")
        print("   2. METRIC 层 → 提取指标并引用已有维度")
        print("   3. metric_dim_rel 中的维度全部来自 dim_definition（非凭空生成）")
        print("\n💡 关键优势：")
        print("   - 维度定义与指标引用分离，职责清晰")
        print("   - 确保 metric_dim_rel 中的维度都是已定义的")
        print("   - 支持分批次处理不同层级的 ETL 脚本")
        print("=" * 80)
    
    except Exception as e:
        print(f"\n❌ 测试异常：{str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()


if __name__ == "__main__":
    test_two_phase_processing()
