"""
测试指标下钻分析器（整合规则引擎）

测试场景：
1. 提取指标名称和用户意图
2. 判断表范围（当前表/上游表）
3. 应用聚合规则到 Prompt
"""

from apps.extend.drilldown.metric_drilldown_handler import MetricDrilldownHandler
from apps.chat.task.llm import get_llm_instance


def test_extract_metrics_and_intent():
    """测试指标和意图提取"""
    print("\n" + "="*60)
    print("测试场景 1：提取指标名称和用户意图")
    print("="*60)
    
    # 创建 LLM 实例
    llm = get_llm_instance()
    
    # 创建分析器
    analyzer = MetricDrilldownHandler()
    
    # 测试用例
    test_cases = [
        {
            "question": "按月下钻指标 d7_sum",
            "expected": {
                "metrics": ["d7_sum"],
                "intent": {
                    "is_granular": True,
                    "is_raw": False,
                    "need_agg": True
                }
            }
        },
        {
            "question": "查询 d7_sum 的明细数据",
            "expected": {
                "metrics": ["d7_sum"],
                "intent": {
                    "is_granular": False,
                    "is_raw": True,
                    "need_agg": False
                }
            }
        },
        {
            "question": "汇总统计销售额和销售量",
            "expected": {
                "metrics": ["销售额", "销售量"],
                "intent": {
                    "is_granular": False,
                    "is_raw": False,
                    "need_agg": True
                }
            }
        },
        {
            "question": "查看用户行为原始明细",
            "expected": {
                "metrics": ["用户行为"],
                "intent": {
                    "is_granular": False,
                    "is_raw": True,
                    "need_agg": False
                }
            }
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n【测试用例 {i}】{test_case['question']}")
        print("-" * 60)
        
        metrics, intent = analyzer.extract_metrics_and_intent(llm, test_case['question'])
        
        print(f"提取的指标：{metrics}")
        print(f"用户意图：{intent}")
        print(f"预期指标：{test_case['expected']['metrics']}")
        print(f"预期意图：{test_case['expected']['intent']}")
        
        # 简单验证
        if metrics == test_case['expected']['metrics']:
            print("✅ 指标提取正确")
        else:
            print("❌ 指标提取错误")
        
        if intent.get('is_granular') == test_case['expected']['intent']['is_granular'] and \
           intent.get('is_raw') == test_case['expected']['intent']['is_raw'] and \
           intent.get('need_agg') == test_case['expected']['intent']['need_agg']:
            print("✅ 意图判断正确")
        else:
            print("❌ 意图判断错误")


def test_judge_table_scope():
    """测试表范围判断"""
    print("\n" + "="*60)
    print("测试场景 2：判断表范围（当前表/上游表）")
    print("="*60)
    
    # 创建 LLM 实例
    llm = get_llm_instance()
    
    # 创建分析器
    analyzer = MetricDrilldownHandler()
    
    # 模拟指标元数据（假设）
    mock_metric_info_list = [
        type('MetricInfo', (), {
            'table_name': 'ads_algo_female_batch_production',
            'dw_layer': 'ADS'
        })()
    ]
    
    # 测试用例
    test_cases = [
        {
            "question": "查询 d7_sum 的汇总数据",
            "expected_is_current": True
        },
        {
            "question": "查看 d7_sum 的原始明细",
            "expected_is_current": False
        },
        {
            "question": "为什么 d7_sum 是这个数，包含哪些",
            "expected_is_current": False
        },
        {
            "question": "本月的 d7_sum 指标",
            "expected_is_current": True
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n【测试用例 {i}】{test_case['question']}")
        print("-" * 60)
        
        table_scope = analyzer.judge_table_scope(llm, test_case['question'], mock_metric_info_list)
        
        print(f"是否当前表：{table_scope['is_current_table']}")
        print(f"目标表：{table_scope['target_tables']}")
        print(f"判断理由：{table_scope['reason']}")
        print(f"预期是否当前表：{test_case['expected_is_current']}")
        
        if table_scope['is_current_table'] == test_case['expected_is_current']:
            print("✅ 表范围判断正确")
        else:
            print("❌ 表范围判断错误")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("指标下钻分析器集成测试（含规则引擎）")
    print("="*80)
    
    try:
        # 测试 1：提取指标和意图
        test_extract_metrics_and_intent()
        
        # 测试 2：判断表范围
        test_judge_table_scope()
        
        print("\n" + "="*80)
        print("✅ 所有测试完成！")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ 测试失败：{str(e)}")
        import traceback
        traceback.print_exc()
