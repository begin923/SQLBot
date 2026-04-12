"""
测试批量解析 SQL 文件

使用方法：
1. 配置根目录路径
2. 运行此脚本
3. 脚本会按 dim → dwd → dws → ads 顺序解析所有SQL文件
"""

import sys
import os
import logging
from pathlib import Path

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


def collect_sql_files(root_dir: str) -> list:
    """
    收集指定目录下的所有SQL文件，按 dim → dwd → dws → ads 顺序
    
    Args:
        root_dir: 根目录路径
        
    Returns:
        SQL文件路径列表（已排序）
    """
    # 定义目录顺序
    dir_order = ['dim', 'dwd', 'dws', 'ads']
    sql_files = []
    
    root_path = Path(root_dir)
    
    if not root_path.exists():
        print(f"❌ 根目录不存在：{root_dir}")
        return []
    
    # 按顺序遍历每个子目录
    for subdir_name in dir_order:
        subdir_path = root_path / subdir_name
        
        if not subdir_path.exists():
            print(f"⚠️  子目录不存在，跳过：{subdir_path}")
            continue
        
        print(f"\n📁 扫描目录：{subdir_path}")
        
        # 递归查找所有 .sql 文件
        sql_files_in_subdir = sorted(subdir_path.rglob('*.sql'))
        
        if sql_files_in_subdir:
            print(f"   找到 {len(sql_files_in_subdir)} 个SQL文件")
            sql_files.extend([str(f) for f in sql_files_in_subdir])
        else:
            print(f"   未找到SQL文件")
    
    print(f"\n✅ 共找到 {len(sql_files)} 个SQL文件")
    return sql_files


def test_parse_batch_sql_files():
    """测试批量解析 SQL 文件（串行执行）"""
    
    # ==================== 配置区域 ====================
    # 根目录（实际SQL文件在 hour 子目录下）
    root_dir = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour"
    
    # 指定层级类型（可选）："DIM", "METRIC", "AUTO"
    layer_type = "AUTO"  # 自动识别
    
    # ==================== 收集文件 ====================
    print("=" * 80)
    print("步骤1: 收集SQL文件")
    print("=" * 80)
    
    sql_files = collect_sql_files(root_dir)
    
    if not sql_files:
        print("\n❌ 没有找到任何SQL文件，退出")
        return
    
    # ==================== 执行测试 ====================
    print("\n" + "=" * 80)
    print("步骤2: 开始解析SQL文件（串行执行）")
    print("=" * 80)
    
    # 获取数据库会话
    session = DBUtils.create_local_session()
    
    # 统计信息
    total_count = len(sql_files)
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    try:
        # 创建服务实例
        platform_service = MetricsPlatformService(session)
        
        # 依次测试每个文件（串行执行，避免大模型调用堆积）
        for file_index, sql_file_path in enumerate(sql_files, 1):
            print("\n" + "=" * 80)
            print(f"进度：[{file_index}/{total_count}]")
            print("=" * 80)
            print(f"📄 文件路径：{sql_file_path}")
            
            # 检查文件是否存在
            if not os.path.exists(sql_file_path):
                print(f"⚠️  文件不存在，跳过")
                skipped_count += 1
                continue
            
            try:
                # 调用解析方法（串行执行，等待完成后再处理下一个）
                print(f"\n🔄 正在解析... (层级类型: {layer_type})")
                result = platform_service.process_metrics_from_sql(
                    input_path=sql_file_path,
                    is_directory=False,
                    layer_type=layer_type
                )
                
                # 打印结果摘要
                if result.get('success'):
                    metrics_count = result.get('metrics_count', 0)
                    sql_count = result.get('generated_sql_count', 0)
                    print(f"✅ 解析成功 | 指标数: {metrics_count} | SQL数: {sql_count}")
                    success_count += 1
                else:
                    print(f"❌ 解析失败: {result.get('message', '未知错误')}")
                    failed_count += 1
            
            except Exception as e:
                print(f"\n❌ 解析异常：{str(e)}")
                failed_count += 1
                # 不打印完整堆栈，保持输出简洁
                # import traceback
                # traceback.print_exc()
        
        # 最终统计
        print("\n" + "=" * 80)
        print("✅ 所有文件解析完成！")
        print("=" * 80)
        print(f"\n📊 最终统计：")
        print(f"   - 总文件数：{total_count}")
        print(f"   - 成功：{success_count}")
        print(f"   - 失败：{failed_count}")
        print(f"   - 跳过：{skipped_count}")
        print("=" * 80)
    
    finally:
        session.close()


if __name__ == "__main__":
    test_parse_batch_sql_files()
