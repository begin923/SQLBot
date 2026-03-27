from apps.extend.utils.utils import Utils


class DrillAggRuleEngine:
    """
    下钻聚合规则引擎
    功能：
    1. 判断下钻类型（粒度下钻/穿透下钻）
    2. 计算聚合规则（是否聚合、如何聚合）
    3. 构建最终 Prompt（注入规则到 LLM 提示词）
    """

    def __init__(self):
        self.rule_keywords = Utils.load_prompt_template_static("rule_keyword")
        # 直接从加载的字典中获取关键词列表
        self.granular_drill_keywords = self.rule_keywords.get("granular_drill_keywords", [])
        self.drill_keywords = self.rule_keywords.get("drill_keywords", [])
        self.aggregate_keywords = self.rule_keywords.get("aggregate_keywords", [])
        self.detail_raw_keywords = self.rule_keywords.get("detail_raw_keywords", [])

    # ====================== 核心1：判断下钻类型（外部生成参数） ======================
    def judge_drill_type(self,question) -> tuple[bool, bool]:
        """
        【前置逻辑】判断用户触发了哪种下钻，返回两个布尔值
        输出：is_granular, is_raw
        """
        q = question.lower()

        # 1. 最高优先级：穿透下钻（查原始明细）
        if any(k in q for k in self.detail_raw_keywords):
            return False, True  # 不是粒度下钻，是穿透下钻

        # 2. 粒度下钻 / 通用下钻（默认归为粒度下钻）
        if any(k in q for k in self.granular_drill_keywords + self.drill_keywords):
            return True, False  # 是粒度下钻，不是穿透下钻

        # 3. 不下钻
        return False, False

    # ====================== 核心2：聚合规则引擎（接收外部传参） ======================
    def get_agg_rule(self,question, curr_layer, is_granular, is_raw):
        q = question.lower()
        # 1. 最高优先级：关键词判断
        if any(k in q for k in self.detail_raw_keywords):
            return "不聚合，查原始明细", False
        if any(k in q for k in self.aggregate_keywords):
            return "强制聚合+GROUP BY", True
        # 2. 次优先级：下钻类型判断（参数由 judge_drill_type 传入）
        if is_granular:
            return "聚合，按维度拆分", True
        if is_raw:
            return "不聚合", False
        #  3. 兜底优先级：数仓分层判断
        if curr_layer.lower().__contains__("dws") or curr_layer.lower("ads"):
            return "强制聚合", True
        else:
            return "不聚合", False

    def build_final_prompt(self,question: str, curr_layer: str) -> str:
        """
        最终Prompt构建：全流程联动
        """
        # 系统硬编码强制规则（禁止修改，底层约束）
        HARD_CODED_RULE_PROMPT = """
        【系统强制聚合规则（不可违反）】
        1. 汇总层(ADS/DWS)必须使用聚合函数+GROUP BY；
        2. 明细层(DWD/ODS)禁止任何聚合函数；
        3. 严格按指令生成SQL，禁止自主推断；
        4. 规则冲突时，优先执行【不聚合】策略。
        """
        # 第一步：前置判断下钻类型
        is_granular, is_raw = self.judge_drill_type(question)
        # 第二步：计算聚合规则
        rule_desc, need_agg = self.get_agg_rule(question, curr_layer, is_granular, is_raw)
        # 第三步：动态指令
        dynamic_instruction = f"【当前指令】{rule_desc}，分层：{curr_layer}"
        # 第四步：拼接注入（硬规则优先级最高）
        final_prompt = f"{HARD_CODED_RULE_PROMPT}\n{dynamic_instruction}"
        return final_prompt

if __name__ == "__main__":
    engine = DrillAggRuleEngine()
    print(engine.build_final_prompt("查询用户行为", "yz_datawarehouse_dws"))
