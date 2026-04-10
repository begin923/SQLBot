"""
测试解析单个 SQL 文件

使用方法：
1. 准备一个 ETL SQL 文件
2. 修改下面的 sql_file_path 为你的文件路径
3. 运行此脚本
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


def test_parse_single_sql_file():
    """测试解析单个 SQL 文件"""
    
    # ==================== 配置区域 ====================
    # 请修改这里为你的 SQL 文件路径
    sql_file_path = "D:/codes/yingzi-data-datawarehouse-release/source/sql/doris/fpf/hour/dws/dws_aib_collect_semen_plan_breed_loc_day.sql"
    
    # 指定层级类型（可选）："DIM", "METRIC", "AUTO"
    layer_type = "AUTO"  # 自动识别，也可以手动指定
    
    # 如果文件不存在，使用示例内容
    if not os.path.exists(sql_file_path):
        print(f"⚠️  文件不存在：{sql_file_path}")
        print("使用内置示例 SQL 进行测试...\n")
        
        # 创建临时测试文件
        sample_sql = """
-- 示例ETL脚本：母猪胎次核心指标统计
INSERT OVERWRITE TABLE dws.dws_pig_parity_di
SELECT
  pig_id,          -- 母猪编号（维度）
  field_id,        -- 猪场编号（维度）
  parity_no,       -- 胎次号（维度）
  dt,              -- 统计日期（维度）
  
  -- 原子指标1：配种总次数
  COUNT(IF(event_type='配种', event_id, NULL)) AS mating_total_cnt,
  
  -- 原子指标2：孕检失败次数
  COUNT(IF(event_type='孕检' AND result='未孕', event_id, NULL)) AS check_fail_cnt,
  
  -- 原子指标3：分娩活仔数
  SUM(IF(event_type='分娩', live_piglet_cnt, 0)) AS live_piglet_cnt,
  
  -- 复合指标：配种失败率
  ROUND(
    COUNT(IF(event_type='孕检' AND result='未孕', event_id, NULL)) / 
    COUNT(IF(event_type='配种', event_id, NULL)), 
    2
  ) AS mating_fail_rate
  
FROM dwd.dwd_pig_breed_event_di
WHERE is_valid = 1
GROUP BY pig_id, field_id, parity_no, dt;
"""
        
        # 写入临时文件
        temp_file = "temp_test_etl.sql"
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(sample_sql)
        sql_file_path = temp_file
        print(f"✅ 已创建临时测试文件：{temp_file}\n")
    
    # ==================== 执行测试 ====================
    print("=" * 80)
    print("开始测试解析单个 SQL 文件")
    print("=" * 80)
    print(f"📄 文件路径：{sql_file_path}\n")
    
    # 获取数据库会话
    session = DBUtils.create_local_session()
    
    try:
        # 创建服务实例
        platform_service = MetricsPlatformService(session)
        
        # 调用解析方法
        print(f"🔄 正在解析 SQL 文件... (层级类型: {layer_type})")
        result = platform_service.process_metrics_from_sql(
            input_path=sql_file_path,
            is_directory=False,
            layer_type=layer_type
        )
        # 打印结果
        print("\n" + "=" * 80)
        print("解析结果")
        print("=" * 80)
        
        if result.get('success'):
            print(f"✅ 解析成功！")
            print(f"\n📊 统计信息：")
            print(f"   - 文件路径：{result.get('file_path')}")
            print(f"   - 识别指标数：{result.get('metrics_count', 0)}")
            print(f"   - 生成SQL数：{result.get('generated_sql_count', 0)}")
            
            # 显示识别的指标
            metrics = result.get('metrics', [])
            if metrics:
                print(f"\n🎯 识别的指标列表：")
                for i, metric in enumerate(metrics, 1):
                    print(f"\n   {i}. {metric.get('metric_name', 'N/A')}")
                    print(f"      - 编码：{metric.get('metric_code', 'N/A')}")
                    print(f"      - 类型：{metric.get('metric_type', 'N/A')}")
                    print(f"      - 计算逻辑：{metric.get('cal_logic', 'N/A')[:100]}...")
                    
                    # 显示维度
                    dims = metric.get('dimensions', [])
                    if dims:
                        print(f"      - 关联维度：{', '.join(dims)}")
                    
                    # 显示数据源
                    sources = metric.get('source_mappings', [])
                    if sources:
                        print(f"      - 数据源表：{sources[0].get('db_table', 'N/A')}")
            
            # 显示生成的 SQL
            insert_sqls = result.get('insert_sqls', [])
            if insert_sqls:
                print(f"\n💾 生成的 INSERT SQL（前3条）：")
                for i, sql in enumerate(insert_sqls[:3], 1):
                    print(f"\n   SQL {i}:")
                    print(f"   {sql[:200]}...")
                
                if len(insert_sqls) > 3:
                    print(f"\n   ... 还有 {len(insert_sqls) - 3} 条 SQL")
            
            print(f"\n{'=' * 80}")
            print("✅ 测试完成！可以查看上方输出的详细信息")
            print("=" * 80)
            
        else:
            print(f"❌ 解析失败！")
            print(f"错误信息：{result}")
    
    except Exception as e:
        print(f"\n❌ 测试异常：{str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()
        
        # 清理临时文件
        if 'temp_file' in locals() and os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"\n🧹 已清理临时文件：{temp_file}")


if __name__ == "__main__":
    test_parse_single_sql_file()
