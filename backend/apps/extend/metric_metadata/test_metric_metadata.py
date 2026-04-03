"""
Metric Metadata 本地测试脚本（直接调用后端接口版本）

使用方法:
    # 在项目根目录或 PyCharm 中运行
    python backend/apps/extend/metric_metadata/test_metric_metadata.py
    
特点:
    ✅ 不需要启动 HTTP 服务
    ✅ 直接调用后端 Python 函数
    ✅ 自动连接数据库
    ✅ 完整的 CRUD 功能测试
    ✅ 详细的错误日志输出

注意:
    - 确保数据库已创建并可以连接
    - 确保已执行建表 SQL (054_create_metric_metadata.sql)
    - EMBEDDING_ENABLED=false 时会自动跳过 embedding 处理
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 现在可以直接导入后端模块
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlmodel import Session
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))
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
session_maker = scoped_session(sessionmaker(bind=engine, class_=Session))


from apps.extend.metric_metadata.curd.metric_metadata import (
    create_metric_metadata,
    batch_create_metric_metadata,
    get_all_metric_metadata,
    page_metric_metadata,
    get_metric_metadata_by_id,
    delete_metric_metadata,
    update_metric_metadata,
    fill_empty_embeddings
)
from apps.extend.metric_metadata.models.metric_metadata_model import MetricMetadataInfo


def print_section(title):
    """打印分隔线"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def test_get_sample_data():
    """1. 获取示例数据"""
    print_section("1. 获取示例数据")
    
    sample_data = [
        {
            "metric_name": "销售额",
            "synonyms": "营收，销售收入，卖钱额，GMV",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "order_id, user_id, amount, pay_time, create_time",
            "calc_logic": "SUM(amount) WHERE pay_time IS NOT NULL",
            "upstream_table": "order_items, cart",
            "dw_layer": "DWS"
        },
        {
            "metric_name": "订单量",
            "synonyms": "订单数，下单数量，成交订单数",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "order_id, user_id, create_time, order_status",
            "calc_logic": "COUNT(DISTINCT order_id) WHERE order_status != 'cancelled'",
            "upstream_table": None,
            "dw_layer": "DWS"
        },
        {
            "metric_name": "客单价",
            "synonyms": "人均消费，ARPU",
            "datasource_id": 1,
            "table_name": "orders",
            "core_fields": "user_id, amount",
            "calc_logic": "SUM(amount) / COUNT(DISTINCT user_id)",
            "upstream_table": "orders",
            "dw_layer": "ADS"
        }
    ]
    
    print(f"✅ 示例数据共 {len(sample_data)} 条")
    print("\n前 3 条数据预览:")
    for i, item in enumerate(sample_data[:3], 1):
        print(f"\n{i}. {item['metric_name']} ({item['synonyms']})")
        print(f"   表：{item['table_name']}, 分层：{item['dw_layer']}")
        print(f"   计算逻辑：{item['calc_logic']}")


def test_insert_sample():
    """2. 插入示例数据"""
    print_section("2. 插入示例数据到数据库")
    
    sample_data = [
        MetricMetadataInfo(
            metric_name="销售额",
            synonyms="营收，销售收入，卖钱额，GMV",
            datasource_id=1,
            table_name="orders",
            core_fields="order_id, user_id, amount, pay_time, create_time",
            calc_logic="SUM(amount) WHERE pay_time IS NOT NULL",
            upstream_table="order_items, cart",
            dw_layer="DWS"
        ),
        MetricMetadataInfo(
            metric_name="订单量",
            synonyms="订单数，下单数量，成交订单数",
            datasource_id=1,
            table_name="orders",
            core_fields="order_id, user_id, create_time, order_status",
            calc_logic="COUNT(DISTINCT order_id) WHERE order_status != 'cancelled'",
            upstream_table=None,
            dw_layer="DWS"
        ),
        MetricMetadataInfo(
            metric_name="客单价",
            synonyms="人均消费，ARPU",
            datasource_id=1,
            table_name="orders",
            core_fields="user_id, amount",
            calc_logic="SUM(amount) / COUNT(DISTINCT user_id)",
            upstream_table="orders",
            dw_layer="ADS"
        ),
        MetricMetadataInfo(
            metric_name="毛利率",
            synonyms="毛利，利润率",
            datasource_id=2,
            table_name="sales_summary",
            core_fields="revenue, cost, profit",
            calc_logic="(revenue - cost) / revenue * 100",
            upstream_table="cost_detail, sales_detail",
            dw_layer="ADS"
        ),
        MetricMetadataInfo(
            metric_name="日活用户",
            synonyms="DAU，活跃用户数",
            datasource_id=1,
            table_name="user_login",
            core_fields="user_id, login_date, login_time",
            calc_logic="COUNT(DISTINCT user_id) WHERE login_date = CURRENT_DATE",
            upstream_table="user_info",
            dw_layer="DWS"
        )
    ]
    
    session = session_maker()
    try:
        result = batch_create_metric_metadata(session, sample_data)
        print(f"✅ 成功插入 {result['success_count']} 条记录")
        if result['duplicate_count'] > 0:
            print(f"⚠️  跳过 {result['duplicate_count']} 条重复记录")
        if result['failed_records']:
            print(f"❌ 失败 {len(result['failed_records'])} 条:")
            for record in result['failed_records']:
                print(f"   - {record['data'].metric_name}: {record['errors']}")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_list_all():
    """3. 查询所有数据"""
    print_section("3. 查询所有指标元数据")
    
    session = session_maker()
    try:
        data = get_all_metric_metadata(session)
        print(f"✅ 共有 {len(data)} 条指标")
        
        if data:
            print("\n指标列表:")
            for i, item in enumerate(data[:10], 1):  # 只显示前 10 条
                print(f"{i}. {item.metric_name} | {item.table_name} | {item.dw_layer}")
            
            if len(data) > 10:
                print(f"... 还有 {len(data) - 10} 条")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_page_query():
    """4. 分页查询"""
    print_section("4. 分页查询 (第 1 页，每页 5 条)")
    
    session = session_maker()
    try:
        current_page, page_size, total_count, total_pages, _list = page_metric_metadata(
            session, current_page=1, page_size=5
        )
        print(f"✅ 当前页：{current_page}/{total_pages}")
        print(f"✅ 总数：{total_count}")
        
        for i, item in enumerate(_list, 1):
            print(f"{i}. {item.metric_name} - {item.synonyms}")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_query_by_name():
    """5. 按名称查询"""
    print_section("5. 模糊查询：'销售'")
    
    session = session_maker()
    try:
        results = get_all_metric_metadata(session, metric_name="销售")
        print(f"✅ 找到 {len(results)} 条包含'销售'的指标")
        for i, item in enumerate(results, 1):
            print(f"{i}. {item.metric_name} ({item.synonyms})")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_create_single():
    """6. 单个创建"""
    print_section("6. 创建单个新指标")
    
    test_data = MetricMetadataInfo(
        metric_name="净利润",
        synonyms="纯利润，净利",
        datasource_id=1,
        table_name="profit_analysis",
        core_fields="revenue, cost, tax, profit",
        calc_logic="revenue - cost - tax",
        upstream_table="sales_detail, cost_detail",
        dw_layer="ADS"
    )
    
    session = session_maker()
    try:
        metric_id = create_metric_metadata(session, test_data)
        print(f"✅ 创建成功！ID={metric_id}")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_update():
    """7. 更新指标"""
    print_section("7. 更新指标（需要先创建）")
    
    session = session_maker()
    try:
        # 先查询是否存在
        metric = get_metric_metadata_by_id(session, 1)
        
        if metric:
            metric.synonyms = metric.synonyms + ", 新增同义词" if metric.synonyms else "新增同义词"
            
            metric_id = update_metric_metadata(session, metric)
            print(f"✅ 更新成功！ID={metric_id}")
        else:
            print("⚠️  未找到可更新的记录，跳过此测试")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()
    finally:
        session_maker.remove()


def test_delete():
    """8. 删除测试"""
    print_section("8. 删除测试（可选）")
    
    print("⚠️  此操作会删除数据，是否继续？(y/n)")
    choice = input("请输入：").strip().lower()
    
    if choice == 'y':
        session = session_maker()
        try:
            # 先查询最后一条记录
            data = get_all_metric_metadata(session)
            
            if data:
                last_id = data[-1].id
                delete_metric_metadata(session, [last_id])
                print(f"✅ 成功删除 ID={last_id}")
            else:
                print("⚠️  没有可删除的记录")
        except Exception as e:
            print(f"❌ 错误：{e}")
            import traceback
            traceback.print_exc()
        finally:
            session_maker.remove()
    else:
        print("⏭️  跳过删除测试")


def test_fill_embeddings(session):
    """9. 填充 Embedding"""
    print_section("9. 填充 Embedding 向量")
    
    print("ℹ️  注意：需要先配置 EMBEDDING_ENABLED=true")
    print("⚠️  此操作可能需要较长时间，请耐心等待...")
    
    try:
        fill_empty_embeddings(session)
        print(f"✅ Embedding 填充完成，请查看日志")
    except Exception as e:
        print(f"❌ 错误：{e}")
        import traceback
        traceback.print_exc()


def run_all_tests():
    """运行所有测试"""
    print("\n" + "🚀" * 30)
    print("  Metric Metadata 本地功能测试")
    print("🚀" * 30)
    
    test_functions = [
        ("获取示例数据", test_get_sample_data),
        ("插入示例数据", test_insert_sample),
        ("查询所有数据", test_list_all),
        ("分页查询", test_page_query),
        ("按名称查询", test_query_by_name),
        ("单个创建", test_create_single),
        ("更新指标", test_update),
        ("删除测试", test_delete),
        ("填充 Embedding", test_fill_embeddings),
    ]
    
    results = []
    
    for name, func in test_functions:
        try:
            func()
            results.append((name, "✅"))
        except Exception as e:
            print(f"\n❌ {name} 执行失败：{e}")
            results.append((name, "❌"))
    
    # 汇总结果
    print_section("测试结果汇总")
    for name, status in results:
        print(f"{status} {name}")
    
    success_count = sum(1 for _, s in results if s == "✅")
    total_count = len(results)
    
    print(f"\n总计：{success_count}/{total_count} 个测试通过")
    print("=" * 60 + "\n")


if __name__ == "__main__":

    test_fill_embeddings(session_maker)
