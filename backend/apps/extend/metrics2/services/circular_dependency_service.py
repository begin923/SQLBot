from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict
import logging

logger = logging.getLogger("CircularDependencyService")


class CircularDependencyService:
    """
    循环依赖检测服务 - 防递归崩溃核心算法
    
    使用 DFS（深度优先搜索）检测有向图中的环路
    用于复合指标计算链路检测，防止 A→B→C→A 这样的死循环
    """

    def __init__(self):
        """初始化循环依赖检测器"""
        self.graph = defaultdict(list)  # 邻接表表示的有向图
        self.visited = set()  # 已访问节点
        self.rec_stack = set()  # 递归栈（当前路径上的节点）
        self.cycle_path = []  # 检测到的环路径

    def add_dependency(self, metric_id: str, sub_metric_id: str):
        """
        添加依赖关系
        
        Args:
            metric_id: 主指标ID
            sub_metric_id: 子指标ID（被依赖）
        """
        self.graph[metric_id].append(sub_metric_id)

    def add_dependencies(self, dependencies: List[Tuple[str, str]]):
        """
        批量添加依赖关系
        
        Args:
            dependencies: 依赖关系列表 [(metric_id, sub_metric_id), ...]
        """
        for metric_id, sub_metric_id in dependencies:
            self.add_dependency(metric_id, sub_metric_id)

    def has_cycle(self) -> bool:
        """
        检测图中是否存在循环依赖
        
        Returns:
            True 表示存在循环依赖，False 表示无循环
        """
        self.visited.clear()
        self.rec_stack.clear()
        self.cycle_path.clear()

        for node in list(self.graph.keys()):
            if node not in self.visited:
                if self._dfs_detect_cycle(node):
                    return True

        return False

    def _dfs_detect_cycle(self, node: str) -> bool:
        """
        DFS 检测环路（递归实现）
        
        Args:
            node: 当前访问的节点
            
        Returns:
            True 表示发现环路，False 表示无环路
        """
        self.visited.add(node)
        self.rec_stack.add(node)
        self.cycle_path.append(node)

        # 遍历所有邻接节点
        for neighbor in self.graph.get(node, []):
            if neighbor not in self.visited:
                if self._dfs_detect_cycle(neighbor):
                    return True
            elif neighbor in self.rec_stack:
                # 发现环路：neighbor 在当前递归栈中
                # 提取环路径
                cycle_start_idx = self.cycle_path.index(neighbor)
                self.cycle_path = self.cycle_path[cycle_start_idx:] + [neighbor]
                logger.error(f"发现循环依赖: {' → '.join(self.cycle_path)}")
                return True

        # 回溯
        self.rec_stack.remove(node)
        self.cycle_path.pop()
        return False

    def get_cycle_path(self) -> List[str]:
        """
        获取检测到的循环路径
        
        Returns:
            循环路径列表，如 ['M001', 'M002', 'M003', 'M001']
        """
        return self.cycle_path.copy()

    def check_single_metric(self, metric_id: str, sub_metrics: List[str]) -> Tuple[bool, List[str]]:
        """
        检查单个指标的依赖是否形成循环
        
        Args:
            metric_id: 指标ID
            sub_metrics: 子指标ID列表
            
        Returns:
            (has_cycle, cycle_path)
        """
        # 清空图并重新构建
        self.graph.clear()
        self.visited.clear()
        self.rec_stack.clear()
        self.cycle_path.clear()

        # 添加当前指标的依赖
        for sub_metric in sub_metrics:
            self.add_dependency(metric_id, sub_metric)

        # 检测循环
        has_cycle = self.has_cycle()
        cycle_path = self.get_cycle_path()

        return has_cycle, cycle_path

    @staticmethod
    def detect_cycle_from_relations(relations: List[Dict]) -> Tuple[bool, List[str]]:
        """
        静态方法：从关系列表直接检测循环
        
        Args:
            relations: 关系列表，格式：[{'metric_id': 'M001', 'sub_metric_id': 'M002'}, ...]
            
        Returns:
            (has_cycle, cycle_path)
        """
        service = CircularDependencyService()
        
        for rel in relations:
            service.add_dependency(rel['metric_id'], rel['sub_metric_id'])

        has_cycle = service.has_cycle()
        cycle_path = service.get_cycle_path()

        return has_cycle, cycle_path

    @staticmethod
    def build_dependency_graph(metrics_with_relations: List[Dict]) -> Dict[str, List[str]]:
        """
        从指标关系数据构建依赖图
        
        Args:
            metrics_with_relations: 指标及其关系列表
                格式：[
                    {'metric_id': 'M001', 'sub_metrics': ['M002', 'M003']},
                    ...
                ]
        
        Returns:
            依赖图（邻接表）：{'M001': ['M002', 'M003'], ...}
        """
        graph = defaultdict(list)

        for metric_data in metrics_with_relations:
            metric_id = metric_data.get('metric_id')
            sub_metrics = metric_data.get('sub_metrics', [])

            if metric_id and sub_metrics:
                for sub_metric in sub_metrics:
                    graph[metric_id].append(sub_metric)

        return dict(graph)

    @staticmethod
    def topological_sort(graph: Dict[str, List[str]]) -> Optional[List[str]]:
        """
        拓扑排序（检测循环的另一种方法）
        
        Args:
            graph: 依赖图（邻接表）
            
        Returns:
            拓扑排序结果，如果存在循环则返回 None
        """
        in_degree = defaultdict(int)
        all_nodes = set()

        # 计算入度
        for node, neighbors in graph.items():
            all_nodes.add(node)
            for neighbor in neighbors:
                in_degree[neighbor] += 1
                all_nodes.add(neighbor)

        # 初始化队列（入度为0的节点）
        queue = [node for node in all_nodes if in_degree[node] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            for neighbor in graph.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # 如果排序结果包含所有节点，说明无循环
        if len(result) == len(all_nodes):
            return result
        else:
            return None  # 存在循环

    def clear(self):
        """清空检测器状态"""
        self.graph.clear()
        self.visited.clear()
        self.rec_stack.clear()
        self.cycle_path.clear()
