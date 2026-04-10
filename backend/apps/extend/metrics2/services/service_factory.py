from typing import Dict, Any
from sqlalchemy.orm import Session
from apps.extend.metrics2.services import MetricService, DimFieldMappingService


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

    def get_metric_service(self) -> MetricService:
        """
        获取指标服务实例

        Returns:
            MetricService 实例
        """
        if 'metric_service' not in self.services:
            self.services['metric_service'] = MetricService(self.session)
        return self.services['metric_service']

    def get_dim_dict_service(self) -> DimFieldMappingService:
        """
        获取维度字典服务实例

        Returns:
            DimDictService 实例
        """
        if 'dim_dict_service' not in self.services:
            self.services['dim_dict_service'] = DimFieldMappingService(self.session)
        return self.services['dim_dict_service']

    def get_service(self, service_name: str) -> Any:
        """
        获取指定服务实例

        Args:
            service_name: 服务名称

        Returns:
            对应的服务实例
        """
        if service_name in self.services:
            return self.services[service_name]
        return None

    def register_service(self, service_name: str, service_instance: Any):
        """
        注册服务实例

        Args:
            service_name: 服务名称
            service_instance: 服务实例
        """
        self.services[service_name] = service_instance

    def close_all_services(self):
        """
        关闭所有服务（如果有需要清理的资源）
        """
        # 这里可以添加服务清理逻辑
        # 例如：关闭数据库连接、释放资源等
        self.services.clear()