"""
测试解析各层 SQL 文件（DIM、DWD、DWS、ADS）

使用方法：
1. 配置各层的SQL文件路径
2. 运行此脚本，会按顺序测试各层
"""

import logging
import os
import sys

# 配置日志（避免重复输出）
logging.basicConfig(
    level=logging.WARNING,  # ⚠️ 默认只输出 WARNING 及以上级别
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 强制重新配置，避免重复handler
)

# 设置特定logger的级别
logging.getLogger("httpx").setLevel(logging.ERROR)  # HTTP请求日志
logging.getLogger("MetricsPlatformService").setLevel(logging.INFO)  # 保留关键信息
logging.getLogger("LineageService").setLevel(logging.WARNING)  # ⚠️ 关闭血缘服务的详细日志
logging.getLogger("MetricsService").setLevel(logging.WARNING)
logging.getLogger("DimService").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)  # ⚠️ 关闭SQLAlchemy的SQL日志
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)  # ⚠️ 关闭连接池日志

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.extend.metrics2.services import MetricsPlatformService
from apps.extend.utils.utils import DBUtils
from apps.extend.metrics2.curd.sql_parse_failure_log_curd import get_failure_statistics, get_failure_logs
from apps.extend.metrics2.curd.sql_parse_success_log_curd import (
    create_or_update_success_log,
    get_success_file_paths,
    get_success_statistics
)
import json
import time

# 断点续传配置文件路径
CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), '.checkpoint.json')

# 明确失败的错误类型（这些错误短期内无法修复，应该跳过）
PERMANENT_FAILURE_ERRORS = [
    'DIM 层未生成任何 dim_definition 数据',  # AI 解析失败
    '❌ DIM 层未生成任何 dim_definition 数据',
]



def save_checkpoint(execution_order: list, current_index: int):
    """
    保存执行进度到检查点文件
    
    Args:
        execution_order: 完整的执行顺序列表
        current_index: 当前处理到的索引（下一个要处理的）
    """
    checkpoint_data = {
        'execution_order': execution_order,
        'current_index': current_index,
        'timestamp': time.time()
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)


def load_checkpoint() -> dict:
    """
    加载检查点文件
    
    Returns:
        检查点数据，如果不存在则返回 None
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  加载检查点文件失败: {e}")
        return None


def clear_checkpoint():
    """清除检查点文件"""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


def test_layer(sql_file_path: str, layer_type: str = "AUTO", session=None, skip_success: bool = True) -> tuple:
    """
    测试单个层级的 SQL 文件解析
    
    Args:
        sql_file_path: SQL 文件路径
        layer_type: 层级类型（AUTO/DIM/DWD/METRIC），默认 AUTO 自动识别
        session: 数据库会话
        skip_success: 是否跳过已成功处理的文件
        
    Returns:
        (success: bool, skipped: bool) - 成功标志和是否跳过
    """
    # ⚠️ 检查是否已成功处理
    if skip_success:
        from apps.extend.metrics2.curd.sql_parse_success_log_curd import get_success_log
        success_log = get_success_log(session, sql_file_path)
        if success_log:
            print(f"\n⏭️  跳过已成功处理的文件")
            print(f"   - 层级类型: {success_log.layer_type}")
            print(f"   - 目标表: {success_log.target_table or 'N/A'}")
            print(f"   - 处理时间: {success_log.parse_time}")
            if success_log.processing_duration:
                print(f"   - 耗时: {success_log.processing_duration:.2f}秒")
            return True, True  # 成功且跳过
    
    print("\n" + "=" * 80)
    print(f"🔍 测试 {layer_type} 层")
    print("=" * 80)
    print(f"📄 文件路径：{sql_file_path}")
    print("=" * 80)
    
    # 检查文件是否存在
    if not os.path.exists(sql_file_path):
        print(f"\n❌ 文件不存在：{sql_file_path}")
        return False, False
    
    start_time = time.time()
    
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
        
        processing_duration = time.time() - start_time
        
        # 打印结果
        if result.get('success'):
            print(f"✅ {layer_type} 层解析成功！")
            print(f"⏱️  耗时: {processing_duration:.2f}秒")
            
            # ⚠️ 如果这个文件之前在失败记录中，标记为已解决
            from apps.extend.metrics2.curd.sql_parse_failure_log_curd import get_failure_logs, mark_as_resolved
            unresolved_failures = get_failure_logs(session, is_resolved=False, limit=1000)
            for failure_log in unresolved_failures:
                if failure_log.file_path == sql_file_path:
                    mark_as_resolved(session, failure_log.id)
                    print(f"✅ 已标记失败记录为已解决 (ID: {failure_log.id})")
                    break
            
            # ⚠️ 记录成功日志（service 层已记录详细信息）
            parsed_results = result.get('parsed_results', [])
            if parsed_results:
                first_result = parsed_results[0]
                parsed_data = first_result.get('parsed_data', {})
                basic_info = parsed_data.get('basic_info', {})
                actual_layer = basic_info.get('warehouse_layer', layer_type).upper()
                target_table = basic_info.get('target_table', '')
                execution_result = result.get('execution_result', {})
                table_stats = execution_result.get('table_stats', {})
                
                create_or_update_success_log(
                    session=session,
                    file_path=sql_file_path,
                    file_name=os.path.basename(sql_file_path),
                    layer_type=actual_layer,
                    target_table=target_table,
                    table_stats=table_stats,
                    processing_duration=processing_duration
                )
                print(f"💾 已记录成功日志")
            
            return True, False
            
        else:
            print(f"❌ {layer_type} 层解析失败！")
            error_msg = result.get('message', '未知错误')
            print(f"错误信息：{error_msg}")
            
            return False, False
    
    except Exception as e:
        # ⚠️ 简化异常输出，不打印完整SQL
        error_type = type(e).__name__
        error_str = str(e)
        
        # 提取简短的错误信息
        if 'StringDataRightTruncation' in error_str:
            error_msg = f"字段长度超限 (VARCHAR(500))"
        elif 'UniqueViolation' in error_str or 'CardinalityViolation' in error_str:
            error_msg = f"唯一约束冲突"
        elif 'StatementError' in error_str or 'InvalidRequestError' in error_str:
            error_msg = f"SQL参数错误"
        else:
            error_msg = f"{error_type}"
        
        print(f"\n❌ {layer_type} 层解析异常：{error_msg}")
        # ⚠️ 不打印 traceback，避免显示完整SQL
        return False, False


def should_skip_permanent_failure(session, file_path: str) -> bool:
    """
    判断文件是否属于明确失败（永久失败），应该跳过
    
    Args:
        session: 数据库会话
        file_path: 文件路径
        
    Returns:
        True 如果是永久失败，应该跳过
    """
    from apps.extend.metrics2.curd.sql_parse_failure_log_curd import get_failure_logs
    
    # 获取该文件的最新失败记录
    unresolved_failures = get_failure_logs(session, is_resolved=False, limit=1000)
    for failure_log in unresolved_failures:
        if failure_log.file_path == file_path:
            error_msg = failure_log.failure_reason or ''  # ⚠️ 修正：使用 failure_reason
            # 检查是否属于永久失败类型
            for permanent_error in PERMANENT_FAILURE_ERRORS:
                if permanent_error in error_msg:
                    return True
    
    return False


def test_all_layers():
    """按顺序测试所有层级（DIM、DWD、DWS、ADS）目录下的所有文件"""
    
    # ==================== 配置区域 ====================
    # 配置根目录路径
    base_dir = r"D:\codes\yingzi-data-datawarehouse-release\source\sql\doris\fpf\hour"
    
    # 定义各层子目录
    layer_dirs = [
        ("dim", os.path.join(base_dir, "dim")),
        ("dwd", os.path.join(base_dir, "dwd")),
        ("dws", os.path.join(base_dir, "dws")),
        ("ads", os.path.join(base_dir, "ads"))
    ]
    
    # 收集所有 SQL 文件
    test_files = []
    for layer_name, layer_dir in layer_dirs:
        if not os.path.exists(layer_dir):
            print(f"⚠️  目录不存在：{layer_dir}")
            continue
        
        # 遍历目录下所有 .sql 文件
        for root, dirs, files in os.walk(layer_dir):
            for file in sorted(files):  # 排序保证顺序一致
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    test_files.append(file_path)
    
    if not test_files:
        print("❌ 未找到任何 SQL 文件")
        return
    
    # ==================== 执行测试 ====================
    print("🚀 开始测试各层 SQL 文件解析")
    print(f"📂 根目录：{base_dir}")
    print(f"📄 共找到 {len(test_files)} 个 SQL 文件")
    print("=" * 80)
    
    # 获取数据库会话
    session = DBUtils.create_local_session()
    
    # ⚠️ 方案 1：加载血缘数据缓存（一次性查询，避免重复）
    from apps.extend.metrics2.utils.lineage_cache import LineageCache
    cache = LineageCache()
    cache.load_all(session)
    cache_stats = cache.get_stats()
    print(f"\n📦 缓存统计: {cache_stats['table_lineage_count']} 条表血缘, {cache_stats['field_lineage_count']} 条字段血缘")
    
    results = {}
    skipped_count = 0
    
    try:
        # ⚠️ 显示失败统计
        print("\n" + "=" * 80)
        print("📊 历史失败统计")
        print("=" * 80)
        failure_stats = get_failure_statistics(session)
        print(f"   - 总失败数: {failure_stats['total_failures']}")
        print(f"   - 未解决: {failure_stats['unresolved_count']}")
        print(f"   - 已解决: {failure_stats['resolved_count']}")
        print(f"   - 解决率: {failure_stats['resolution_rate']}")
        if failure_stats['error_type_distribution']:
            print(f"\n   错误类型分布:")
            for error_type, count in sorted(failure_stats['error_type_distribution'].items()):
                print(f"      - {error_type}: {count}")
        
        # ⚠️ 显示成功统计
        print("\n" + "=" * 80)
        print("📊 历史成功统计")
        print("=" * 80)
        success_stats = get_success_statistics(session)
        print(f"   - 总成功数: {success_stats['total_success']}")
        if success_stats['layer_type_distribution']:
            print(f"\n   层级类型分布:")
            for layer_type, count in sorted(success_stats['layer_type_distribution'].items()):
                print(f"      - {layer_type}: {count}")
        print(f"   - 平均耗时: {success_stats['avg_processing_duration']}")
        
        # ⚠️ 获取已成功处理的文件列表
        success_file_paths = get_success_file_paths(session)
        remaining_files = [f for f in test_files if f not in success_file_paths]
        
        # ⚠️ 获取未解决的失败记录，优先处理
        unresolved_failures = get_failure_logs(session, is_resolved=False, limit=1000)
        failed_file_paths = {log.file_path for log in unresolved_failures}
        
        # ⚠️ 过滤掉明确失败的文件（永久失败）
        permanent_failure_files = set()
        for file_path in remaining_files:
            if should_skip_permanent_failure(session, file_path):
                permanent_failure_files.add(file_path)
        
        # 将失败文件放在最前面优先处理（排除永久失败）
        priority_files = [f for f in remaining_files if f in failed_file_paths and f not in permanent_failure_files]
        normal_files = [f for f in remaining_files if f not in failed_file_paths and f not in permanent_failure_files]
        skipped_permanent = len(permanent_failure_files)
        
        # 合并：优先文件 + 正常文件
        execution_order = priority_files + normal_files
        
        # ⚠️ 检查是否有检查点（断点续传）
        checkpoint = load_checkpoint()
        start_index = 0
        if checkpoint and checkpoint.get('execution_order') == execution_order:
            start_index = checkpoint.get('current_index', 0)
            print(f"\n🔄 检测到检查点，从第 {start_index + 1}/{len(execution_order)} 个文件继续执行")
            print(f"   - 上次执行时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(checkpoint.get('timestamp', 0)))}")
        else:
            # 如果没有检查点或执行顺序不同，从头开始
            if checkpoint:
                print(f"\n⚠️  执行顺序已变化，重新开始执行")
            clear_checkpoint()
        
        print("\n" + "=" * 80)
        print(f"📋 本次执行计划")
        print("=" * 80)
        print(f"   - 总文件数: {len(test_files)}")
        print(f"   - 已成功: {len(success_file_paths)}")
        print(f"   - 永久失败（跳过）: {skipped_permanent}")
        print(f"   - 待处理: {len(remaining_files) - skipped_permanent}")
        if priority_files:
            print(f"   - 🔄 优先处理失败文件: {len(priority_files)} 个")
            print(f"   - 📄 普通文件: {len(normal_files)} 个")
        print("=" * 80)
        
        # 按顺序测试所有文件（使用 AUTO 模式自动识别）
        for idx, file_path in enumerate(execution_order, 1):
            # ⚠️ 跳过检查点之前的文件
            if idx <= start_index:
                continue
            
            # 标记是否是优先处理的失败文件
            is_priority = file_path in failed_file_paths
            priority_tag = " 🔄[优先]" if is_priority else ""
            
            print(f"\n{'='*80}")
            print(f"📋 测试文件 {idx}/{len(execution_order)}{priority_tag}")
            print(f"{'='*80}")
            
            success, skipped = test_layer(file_path, layer_type="AUTO", session=session, skip_success=True)
            results[file_path] = success
            
            if skipped:
                skipped_count += 1
            
            # ⚠️ 每处理一个文件就保存检查点
            save_checkpoint(execution_order, idx)
        
        # 打印总结
        print("\n" + "=" * 80)
        print("📊 测试总结")
        print("=" * 80)
        
        success_count = sum(1 for v in results.values() if v)
        failed_count = len(results) - success_count
        
        print(f"   - 总文件数: {len(test_files)}")
        print(f"   - 跳过成功: {skipped_count}")
        print(f"   - 本次成功: {success_count - skipped_count}")
        print(f"   - 本次失败: {failed_count}")
        
        if failed_count > 0:
            print(f"\n⚠️  以下文件测试失败:")
            for file_path, success in results.items():
                if not success:
                    file_name = os.path.basename(file_path)
                    is_priority = file_path in failed_file_paths
                    tag = " 🔄[曾失败]" if is_priority else ""
                    print(f"   ❌ {file_name}{tag}")
        else:
            print("\n🎉 所有文件测试通过！")
        
        # ⚠️ 再次显示失败统计，对比变化
        print("\n" + "=" * 80)
        print("📊 最新失败统计")
        print("=" * 80)
        failure_stats_after = get_failure_statistics(session)
        print(f"   - 总失败数: {failure_stats_after['total_failures']}")
        print(f"   - 未解决: {failure_stats_after['unresolved_count']}")
        print(f"   - 已解决: {failure_stats_after['resolved_count']}")
        print(f"   - 解决率: {failure_stats_after['resolution_rate']}")
        
        # ⚠️ 对比分析
        new_failures = failure_stats_after['total_failures'] - failure_stats['total_failures']
        resolved_failures = failure_stats['unresolved_count'] - failure_stats_after['unresolved_count']
        
        print("\n" + "=" * 80)
        print("📈 本次执行效果分析")
        print("=" * 80)
        print(f"   - 新增失败: {new_failures} 个")
        print(f"   - 解决失败: {resolved_failures} 个")
        if resolved_failures > 0:
            print(f"   ✅ 成功解决了 {resolved_failures} 个之前的失败问题！")
        if new_failures == 0 and resolved_failures >= 0:
            print(f"   🎉 没有新增失败，问题解决进展良好！")
        
    finally:
        session.close()
        # ⚠️ 执行完成后清除检查点
        clear_checkpoint()
        print("\n" + "=" * 80)


if __name__ == "__main__":
    test_all_layers()
