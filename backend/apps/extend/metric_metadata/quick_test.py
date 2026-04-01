"""
Metric Metadata 快速验证脚本

功能：
    - 最小化的测试，只验证核心功能
    - 快速检查后端接口是否可用
    - 适合开发过程中的快速迭代测试

使用方法:
    python backend/apps/extend/metric_metadata/quick_test.py
"""

import sys
from pathlib import Path

# 获取项目根目录（向上追溯 4 层）
from apps.extend.format.parse_md_to_json import ParseMDToJson
from apps.extend.metric_metadata.curd.metric_metadata import fill_empty_embeddings, get_metric_metadata_by_names

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# 直接导入需要的模块，避免循环依赖
from sqlalchemy import create_engine, Column, BigInteger, String, Text, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from datetime import datetime
from typing import List, Optional

# 导入正式的模型定义
from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadataInfo, MetricMetadata

# ========== 数据库配置 ==========
# 从根目录的.env 文件读取数据库连接
import os
from dotenv import load_dotenv

# 获取项目根目录（向上追溯 5 层：backend/apps/extend/metric_metadata -> D:\codes\MySQLBot）
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent.parent.parent

# 加载根目录的.env 文件
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"✅ 已加载.env 文件：{env_path}")
    print(f"   POSTGRES_SERVER={os.getenv('POSTGRES_SERVER')}")
    print(f"   POSTGRES_DB={os.getenv('POSTGRES_DB')}")
else:
    print(f"⚠️  未找到.env 文件：{env_path}")
    # 尝试当前工作目录
    alt_env_path = Path.cwd() / ".env"
    if alt_env_path.exists() and str(alt_env_path) != str(env_path):
        load_dotenv(alt_env_path)
        print(f"✅ 已加载备用.env 文件：{alt_env_path}")
    else:
        print("ℹ️  将使用默认数据库配置")

# 从环境变量读取数据库配置（使用.env 文件中的变量名）
DB_HOST = os.getenv("POSTGRES_SERVER", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "sqlbot")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sqlbot")
DB_NAME = os.getenv("POSTGRES_DB", "sqlbot")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"📡 数据库连接：{DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"👤 用户：{DB_USER}")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def create_metric_metadata(session: Session, info: MetricMetadataInfo):
    """创建单个指标记录（纯测试用，不处理 embedding）"""
    # 检查是否已存在（使用 metric_column + table_name 匹配，与唯一索引一致）
    existing = session.query(MetricMetadata).filter(
        MetricMetadata.metric_column == info.metric_column,
        MetricMetadata.table_name == info.table_name
    ).first()
    
    if existing:
        raise Exception(f"指标 {info.metric_name} 在表 {info.table_name} 中已存在 (字段：{info.metric_column})")
    
    metric = MetricMetadata(
        metric_name=info.metric_name,
        metric_column=info.metric_column,
        synonyms=info.synonyms,
        datasource_id=info.datasource_id,
        table_name=info.table_name,
        core_fields=info.core_fields,
        calc_logic=info.calc_logic,
        upstream_table=info.upstream_table,
        dw_layer=info.dw_layer,
        create_time=datetime.now()
    )
    
    session.add(metric)
    session.commit()
    session.refresh(metric)
    return metric.id


def batch_create_metric_metadata(session: Session, info_list: List[MetricMetadataInfo]):
    """批量创建指标记录（纯测试用）"""
    success_count = 0
    failed_records = []
    duplicate_records = []
    duplicate_count = 0
    
    for info in info_list:
        try:
            metric_id = create_metric_metadata(session, info)
            if metric_id is None:
                # 记录已存在
                duplicate_count += 1
                duplicate_records.append({
                    'data': info,
                    'errors': [f"记录已存在 (metric_column={info.metric_column}, table_name={info.table_name})"],
                    'table_name': info.table_name,
                    'metric_column': info.metric_column,
                    'dw_layer': info.dw_layer
                })
            else:
                success_count += 1
        except Exception as e:
            error_msg = str(e)
            # 提取更详细的信息
            failed_records.append({
                'data': info,
                'errors': [error_msg],
                'table_name': info.table_name,
                'metric_column': info.metric_column,
                'dw_layer': info.dw_layer
            })
    
    # 打印详细统计
    total = len(info_list)
    failed_count = len(failed_records)
    print(f"\n📊 批量创建详细统计:")
    print(f"   总指标数：{total}")
    print(f"   成功创建：{success_count}")
    print(f"   重复跳过：{duplicate_count}")
    print(f"   其他失败：{failed_count}")
    
    # 显示重复的记录
    if duplicate_count > 0:
        print(f"\n🔄 重复记录详情 (共 {duplicate_count} 条):")
        for i, record in enumerate(duplicate_records[:10], 1):  # 最多显示 10 条
            print(f"      {i}. 指标：{record['data'].metric_name}")
            print(f"         字段：{record['metric_column']}")
            print(f"         表：{record['table_name']}")
            print(f"         层级：{record['dw_layer']}")
    
    # 显示失败的记录
    if failed_count > 0:
        print(f"\n⚠️  失败示例 (前 5 条):")
        for i, record in enumerate(failed_records[:5], 1):
            print(f"      {i}. 指标：{record['data'].metric_name}")
            print(f"         字段：{record['metric_column']}")
            print(f"         表：{record['table_name']}")
            print(f"         层级：{record['dw_layer']}")
            print(f"         错误：{record['errors'][0][:200]}")
    
    return {
        'success_count': success_count,
        'failed_records': failed_records,
        'duplicate_count': duplicate_count,
        'original_count': len(info_list)
    }


def get_all_metric_metadata(session: Session, metric_name: str = None):
    """查询所有指标"""
    query = session.query(MetricMetadata)
    
    if metric_name:
        query = query.filter(MetricMetadata.metric_name.like(f"%{metric_name}%"))
    
    results = query.order_by(MetricMetadata.create_time.desc()).all()
    
    # 转换为 Pydantic 对象
    return [
        MetricMetadataInfo(
            metric_name=r.metric_name,
            metric_column=r.metric_column,
            synonyms=r.synonyms,
            datasource_id=r.datasource_id,
            table_name=r.table_name,
            core_fields=r.core_fields,
            calc_logic=r.calc_logic,
            upstream_table=r.upstream_table,
            dw_layer=r.dw_layer,
            embedding_vector=r.embedding_vector
        )
        for r in results
    ]


def test_create_single_metric(session: Session):
    """测试 1：创建单个指标"""
    print("\n[测试 1] 创建单个指标...")
    
    test_metric = MetricMetadataInfo(
        metric_name="猪精国内销售量",
        synonyms="测试营收",
        datasource_id=999,
        table_name="yz_datawarehouse_dws.dws_inb_pig_semen_product_breed_loc_day",
        core_fields="id, amount",
        calc_logic="SUM(amount)",
        dw_layer="yz_datawarehouse_dws"
    )
    
    try:
        metric_id = create_metric_metadata(session, test_metric)
        print(f"✅ 创建成功！ID = {metric_id}")
        return True
    except Exception as e:
        if "已存在" in str(e):
            print(f"⚠️  数据已存在，跳过创建")
            return True
        else:
            print(f"❌ 创建失败：{e}")
            return False


def test_batch_create_metrics(session: Session):
    """测试 2：批量创建指标"""
    print("\n[测试 2] 批量创建指标...")
    
    batch_data = [
        MetricMetadataInfo(
            metric_name=f"批量测试指标_{i}",
            synonyms=f"同义词{i}",
            datasource_id=999,
            table_name=f"test_table_{i}",
            core_fields="id, value",
            calc_logic=f"SUM(value_{i})",
            dw_layer="ODS"
        )
        for i in range(3)
    ]
    
    result = batch_create_metric_metadata(session, batch_data)
    print(f"✅ 批量创建成功！成功 {result['success_count']} 条，重复 {result['duplicate_count']} 条")
    return True


def test_query_all_metrics(session: Session):
    """测试 3：查询所有指标"""
    print("\n[测试 3] 查询所有指标...")
    
    all_metrics = get_all_metric_metadata(session)
    print(f"✅ 数据库中共有 {len(all_metrics)} 条指标")
    
    # 显示最新创建的 5 条
    recent_metrics = all_metrics[:5] if len(all_metrics) > 5 else all_metrics
    print("\n最新创建的指标:")
    for i, metric in enumerate(recent_metrics, 1):
        has_embedding = "✅" if metric.embedding_vector else "❌"
        print(f"  {i}. {metric.metric_name} | {metric.table_name} | {metric.dw_layer} | Embedding: {has_embedding}")
    
    return True


def test_vectorization(session: Session):
    """测试 4：向量化处理"""
    print("\n[测试 4] 调用后端函数进行向量化...")
    
    try:
        # 先检查表和数据
        from sqlalchemy import inspect, text
        inspector = inspect(engine)
        
        if 'metric_metadata' not in inspector.get_table_names():
            print(f"⚠️  metric_metadata 表不存在，跳过向量化")
            return True
        
        columns = [col['name'] for col in inspector.get_columns('metric_metadata')]
        if 'embedding_vector' not in columns:
            print(f"⚠️  embedding_vector 字段不存在，跳过向量化")
            return True
        
        # 统计缺失情况
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM metric_metadata WHERE embedding_vector IS NULL"))
            null_count = result.scalar()
        
        if null_count > 0:
            print(f"📊 发现 {null_count} 条记录需要向量化")
            
            # 调用 CRUD 中的向量化函数
            import time
            start_time = time.time()
            fill_empty_embeddings()
            elapsed_time = time.time() - start_time
            
            print(f"   ✅ 向量化完成！耗时：{elapsed_time:.2f}秒")
            
            # 验证结果
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM metric_metadata WHERE embedding_vector IS NOT NULL"))
                vectorized_count = result.scalar()
                result_total = conn.execute(text("SELECT COUNT(*) FROM metric_metadata"))
                total_count = result_total.scalar()
            print(f"   📊 已向量化 {vectorized_count}/{total_count} 条记录")
        else:
            print("✅ 所有记录已完成向量化")
        
        return True
    except Exception as e:
        print(f"   ⚠️  向量化失败：{e}")
        import traceback
        traceback.print_exc()
        return False


def test_conditional_query(session: Session):
    """测试 5：条件查询"""
    print("\n[测试 5] 条件查询（模糊匹配'测试'）...")
    
    test_metrics = get_all_metric_metadata(session, metric_name="测试")
    print(f"✅ 找到 {len(test_metrics)} 条包含'测试'的指标")
    return True


def test_mixed_query(session: Session):
    """测试 6：混合查询（模糊匹配 + 向量相似度）"""
    print("\n[测试 6] 测试 get_metric_metadata_by_names 函数...")
    
    try:
        # 6.1 测试单个指标名称查询
        print("\n  6.1 测试单个指标名称查询...")
        results = get_metric_metadata_by_names(session, ["猪精国内销售量"])
        print(f"  ✅ 查询到 {len(results)} 条结果")
        for i, result in enumerate(results[:3], 1):
            print(f"     {i}. {result.metric_name} | {result.table_name}")
        
        # 6.2 测试多个指标名称查询
        print("\n  6.2 测试多个指标名称查询...")
        results = get_metric_metadata_by_names(session, ["猪精", "批量测试"])
        print(f"  ✅ 查询到 {len(results)} 条结果")
        for i, result in enumerate(results[:5], 1):
            print(f"     {i}. {result.metric_name} | {result.table_name}")
        
        # 6.3 测试带数据源过滤的查询
        print("\n  6.3 测试带数据源过滤的查询...")
        results = get_metric_metadata_by_names(session, ["测试"], datasource_id=999)
        print(f"  ✅ 查询到 {len(results)} 条结果（datasource_id=999）")
        for i, result in enumerate(results[:3], 1):
            print(f"     {i}. {result.metric_name} | datasource_id={result.datasource_id}")
        
        # 6.4 测试空列表
        print("\n  6.4 测试空列表输入...")
        results = get_metric_metadata_by_names(session, [])
        print(f"  ✅ 空列表返回 {len(results)} 条结果")
        
        # 6.5 测试不存在的指标名称
        print("\n  6.5 测试不存在的指标名称...")
        results = get_metric_metadata_by_names(session, ["不存在的指标_xyz_123"])
        print(f"  ✅ 不存在的指标返回 {len(results)} 条结果")
        
        print("\n  🎉 get_metric_metadata_by_names 函数测试完成！")
        return True
    except Exception as e:
        print(f"  ❌ 测试失败：{e}")
        import traceback
        traceback.print_exc()
        return False


def quick_test():
    """快速测试核心功能（整合所有测试）"""
    
    print("=" * 60)
    print("  Metric Metadata 快速验证测试")
    print("=" * 60)
    print(f"\n数据库连接：{DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else DATABASE_URL}")
    
    session = SessionLocal()
    test_results = []
    
    try:
        # 执行所有测试
        test_results.append(("单个创建", test_create_single_metric(session)))
        test_results.append(("批量创建", test_batch_create_metrics(session)))
        test_results.append(("查询所有", test_query_all_metrics(session)))
        test_results.append(("向量化处理", test_vectorization(session)))
        test_results.append(("条件查询", test_conditional_query(session)))
        test_results.append(("混合查询", test_mixed_query(session)))
        
        # 汇总结果
        print("\n" + "=" * 60)
        print("  测试结果汇总")
        print("=" * 60)
        
        passed_count = sum(1 for _, result in test_results if result)
        for test_name, result in test_results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{status} - {test_name}")
        
        print(f"\n总计：{passed_count}/{len(test_results)} 个测试通过")
        
        if passed_count == len(test_results):
            print("\n🎉 所有测试通过！后端功能正常！")
        else:
            print(f"\n⚠️  有 {len(test_results) - passed_count} 个测试失败，请检查")
        
        print("=" * 60 + "\n")
        
        return passed_count == len(test_results)
        
    except Exception as e:
        print(f"\n❌ 测试失败：{e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        session.close()


if __name__ == "__main__":
    # ========== 选项 1: 运行完整测试 ==========
    # success = quick_test()
    # if success:
    #     print("✅ 验证完成，可以开始使用 Metric Metadata 模块")
    # else:
    #     print("❌ 验证失败，请检查数据库连接和表结构")

    # ========== 选项 2: 仅执行向量化 ==========
    # fill_empty_embeddings()  # 直接调用 CRUD 中的函数
    # print("\n✅ 向量化完成！")

    # ========== 选项 3: 批量创建指标元数据 ==========
    session = SessionLocal()
    parse = ParseMDToJson()
    res = parse.parse_md_to_metric_metadata_list()
    batch_data = res
    result = batch_create_metric_metadata(session, batch_data)
    print(f"✅ 批量创建成功！成功 {result['success_count']} 条，重复 {result['duplicate_count']} 条")
    session.close()
