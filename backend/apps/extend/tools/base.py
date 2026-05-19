"""
工具注册器和基类
"""
from typing import Callable, Dict, Any, Optional
from abc import ABC, abstractmethod
import json


class ToolRegistry:
    """全局工具注册器"""
    _tools: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def register(cls, name: str, description: str, parameters: dict):
        """
        注册工具函数
        
        Args:
            name: 工具名称（英文，用于 LLM 调用）
            description: 工具描述（中文，告诉 LLM 这个工具做什么）
            parameters: JSON Schema 格式的参数定义
        """
        def decorator(func: Callable):
            # 保存工具元数据
            cls._tools[name] = {
                'function': func,
                'name': name,
                'description': description,
                'parameters': parameters
            }
            
            # 附加到函数对象上，方便调试
            func._tool_metadata = {
                'name': name,
                'description': description,
                'parameters': parameters
            }
            
            return func
        return decorator
    
    @classmethod
    def get_tool(cls, name: str) -> Optional[Callable]:
        """获取已注册的工具函数"""
        tool_info = cls._tools.get(name)
        return tool_info['function'] if tool_info else None
    
    @classmethod
    def get_tool_metadata(cls, name: str) -> Optional[dict]:
        """获取工具的元数据（用于构建 OpenAI function calling schema）"""
        tool_info = cls._tools.get(name)
        if not tool_info:
            return None
        
        return {
            'type': 'function',
            'function': {
                'name': tool_info['name'],
                'description': tool_info['description'],
                'parameters': tool_info['parameters']
            }
        }
    
    @classmethod
    def get_all_tools_metadata(cls) -> list:
        """获取所有工具的元数据列表（用于初始化 LLM）"""
        result = []
        for name, info in cls._tools.items():
            result.append({
                'type': 'function',
                'function': {
                    'name': info['name'],
                    'description': info['description'],
                    'parameters': info['parameters']
                }
            })
        return result
    
    @classmethod
    def call_tool(cls, name: str, **kwargs) -> Any:
        """
        调用指定的工具
        
        Args:
            name: 工具名称
            **kwargs: 传递给工具的参数
            
        Returns:
            工具执行结果
        """
        func = cls.get_tool(name)
        if not func:
            raise ValueError(f"未找到工具: {name}")
        
        try:
            return func(**kwargs)
        except Exception as e:
            raise RuntimeError(f"工具执行失败 [{name}]: {str(e)}")
    
    @classmethod
    def clear(cls):
        """清空所有注册的工具（用于测试）"""
        cls._tools.clear()


class BaseTool(ABC):
    """工具基类（可选，用于更复杂的工具实现）"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """工具名称"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """工具描述"""
        pass
    
    @property
    @abstractmethod
    def parameters(self) -> dict:
        """参数 schema"""
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """执行工具"""
        pass
    
    def to_metadata(self) -> dict:
        """转换为元数据"""
        return {
            'type': 'function',
            'function': {
                'name': self.name,
                'description': self.description,
                'parameters': self.parameters
            }
        }
