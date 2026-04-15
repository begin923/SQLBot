from typing import Dict, Any
from sqlalchemy.orm import Session

class ServiceFactory:
    """服务工厂类 - 负责创建和管理服务实例"""

    def __init__(self, session: Session):
        """
        初始化服务工厂

        Args:
            session: 数据库会话
        """
        self.session = session
        self.services: Dict[str, Any] = {}
