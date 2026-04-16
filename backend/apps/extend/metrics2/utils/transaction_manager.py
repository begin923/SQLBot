"""
事务管理器工具类 - 提供统一的事务管理功能
"""

import logging
from typing import Generator, TypeVar, Type, Any
from contextlib import contextmanager
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


T = TypeVar('T')


class TransactionManager:
    """
    事务管理器，提供统一的事务管理功能

    使用示例：
    ```python
    # 使用上下文管理器
    with TransactionManager(session) as transaction:
        # 在事务中执行操作
        result1 = some_operation1()
        result2 = some_operation2()
        # 事务会在退出时自动提交或回滚

    # 手动管理事务
    transaction = TransactionManager(session)
    transaction.begin()
    try:
        # 执行操作
        transaction.commit()
    except Exception as e:
        transaction.rollback()
        raise
    ```

    特性：
    1. 自动事务管理（提交/回滚）
    2. 支持嵌套事务
    3. 异常处理
    4. 状态检查
    """

    def __init__(self, session: Session):
        """
        初始化事务管理器

        Args:
            session: 数据库会话
        """
        self.session = session
        self._nested_level = 0

    def __enter__(self) -> 'TransactionManager':
        """进入上下文管理器"""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def begin(self):
        """开始事务"""
        if self._nested_level == 0:
            self.session.begin()
        self._nested_level += 1

    def commit(self):
        """提交事务"""
        self._nested_level -= 1
        if self._nested_level == 0:
            try:
                self.session.commit()
                logger.debug("[TransactionManager] 事务提交成功")
            except Exception as e:
                logger.error(f"[TransactionManager] 事务提交失败: {str(e)}")
                raise

    def rollback(self):
        """回滚事务"""
        self._nested_level -= 1
        if self._nested_level == 0:
            try:
                self.session.rollback()
                logger.warning("[TransactionManager] 事务已回滚")
            except Exception as e:
                logger.error(f"[TransactionManager] 事务回滚失败: {str(e)}")
                raise

    def in_transaction(self) -> bool:
        """检查是否在事务中"""
        return self.session.in_transaction()

    def execute(self, operation: callable, *args, **kwargs) -> Any:
        """
        在事务中执行操作

        Args:
            operation: 要执行的操作函数
            *args: 操作函数的参数
            **kwargs: 操作函数的关键字参数

        Returns:
            操作的结果
        """
        self.begin()
        try:
            result = operation(*args, **kwargs)
            self.commit()
            return result
        except Exception as e:
            self.rollback()
            raise

    @contextmanager
    def transaction_context(self):
        """
        事务上下文管理器

        使用示例：
        ```python
        with TransactionManager(session).transaction_context() as transaction:
            # 在事务中执行操作
            result = some_operation()
        ```
        """
        self.begin()
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            raise

    def savepoint(self, name: str = "savepoint"):
        """
        创建保存点

        Args:
            name: 保存点名称

        Returns:
            保存点对象
        """
        return self.session.begin_nested()

    def rollback_to_savepoint(self, savepoint):
        """回滚到保存点"""
        savepoint.rollback()

    def release_savepoint(self, savepoint):
        """释放保存点"""
        savepoint.release()

    def execute_with_savepoint(self, operation: callable, *args, **kwargs) -> Any:
        """
        使用保存点执行操作

        Args:
            operation: 要执行的操作函数
            *args: 操作函数的参数
            **kwargs: 操作函数的关键字参数

        Returns:
            操作的结果
        """
        savepoint = self.savepoint()
        try:
            result = operation(*args, **kwargs)
            self.release_savepoint(savepoint)
            return result
        except Exception as e:
            self.rollback_to_savepoint(savepoint)
            raise

    def reset_session(self):
        """重置会话（主要用于测试）"""
        self.session.rollback()
        self._nested_level = 0

    def get_session(self) -> Session:
        """获取当前会话"""
        return self.session

    def set_session(self, session: Session):
        """设置新的会话"""
        self.session = session
        self._nested_level = 0