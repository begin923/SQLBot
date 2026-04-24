"""
表元数据服务 - 统一处理 table_metadata 的写入逻辑
"""

from typing import Dict, Any
import logging
from sqlalchemy.orm import Session

logger = logging.getLogger("TableMetadataService")


class TableMetadataService:
    """表元数据服务"""
    
    def __init__(self, session: Session):
        """
        初始化表元数据服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
    
    def process_table_metadata(self, processed_result: Dict[str, Any], service_name: str = "Unknown", layer_type: str = "AUTO"):
        """
        处理表元数据（从 AI 解析的 basic_info 中提取）
        ⚠️ 此方法应该在所有主要数据写入成功后调用
        ⚠️ 如果写入失败，会抛出异常，由主流程统一回滚
        
        Args:
            processed_result: 单个文件的处理结果
            service_name: 调用方服务名称（用于日志标识）
            layer_type: 数仓层级类型（DIM/DWD/DWS/ADS），用于统一推断 source_level
            
        Raises:
            Exception: 当 table_metadata 写入失败时抛出异常
        """
        from apps.extend.metrics2.curd.table_metadata_curd import get_or_create_table_metadata
        
        parsed_data = processed_result.get('parsed_data', {})
        basic_info = parsed_data.get('basic_info', {})
        
        # 提取表信息
        target_table = basic_info.get('target_table', '')
        table_desc = basic_info.get('table_desc', '')
        
        # ⚠️ 统一使用 MetricsPlatformService 传入的 layer_type 作为 source_level
        # 保证全局一致性，不依赖 AI 返回的 warehouse_layer
        inferred_source_level = layer_type.upper() if layer_type != "AUTO" else basic_info.get('warehouse_layer', '').upper()
        
        if not target_table:
            logger.warning(f"[{service_name}-表元数据] ⚠️ 未找到 target_table，跳过")
            return
        
        # 调用 CURD 创建或更新表元数据（如果失败会抛出异常）
        table_metadata_id = get_or_create_table_metadata(
            session=self.session,
            table_name=target_table,
            source_level=inferred_source_level,  # ⚠️ 使用统一推断的分层
            biz_domain=None,  # TODO: 可以从 basic_info 或其他地方提取业务域
            table_comment=table_desc
        )
        
        logger.info(f"[{service_name}-表元数据] ✅ {target_table} -> ID: {table_metadata_id} (layer: {inferred_source_level})")
