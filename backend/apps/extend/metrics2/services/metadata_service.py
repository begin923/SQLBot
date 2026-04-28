"""
表元数据服务 - 统一处理 table_metadata 的数据收集逻辑
"""

from typing import Dict, Any, List
import logging
from sqlalchemy.orm import Session

# ⚠️ 使用根 logger，继承 basicConfig 的配置
from apps.extend.metrics2.utils.id_generator import IdGenerator
from apps.extend.metrics2.utils.timezone_helper import get_now_utc8

logger = logging.getLogger(__name__)


class metadataService:
    """表元数据服务 - 负责收集 table_metadata 数据"""
    
    def __init__(self, session: Session):
        """
        初始化表元数据服务
        
        Args:
            session: 数据库会话
        """
        self.session = session
        # 生成 table_metadata_id（使用 IdGenerator）
        self.id_gen = IdGenerator(self.session, 'table_metadata', 'TM')
    
    def collect_table_metadata(self, processed_result: Dict[str, Any], table_data: Dict[str, List], layer_type: str):
        """
        收集表元数据（从 AI 解析的 basic_info 中提取）
        ⚠️ 此方法将数据添加到 table_data['table_metadata'] 中，由主流程统一批量插入
        
        Args:
            processed_result: 单个文件的处理结果
            table_data: 表数据收集字典
            layer_type: 数仓层级类型（DIM/DWD/DWS/ADS），用于统一推断 source_level
        """
        parsed_data = processed_result.get('parsed_data', {})
        basic_info = parsed_data.get('basic_info', {})
        
        # 提取表信息
        target_table = basic_info.get('target_table', '')
        table_desc = basic_info.get('table_desc', '')
        file_path = basic_info.get('file_name', '')  # ⚠️ 提取文件路径
        
        if not target_table:
            logger.warning("[metadataService] ⚠️ target_table is null，跳过 table_metadata")
            return

        table_metadata_id = self.id_gen.get_next_id()
        
        now = get_now_utc8()
        
        # ⚠️ 关键修复：将 table_metadata 数据添加到 table_data
        table_data['table_metadata'].append({
            'id': table_metadata_id,
            'table_name': target_table,
            'source_level': layer_type,
            'biz_domain': None,  # TODO: 可以从 basic_info 或其他地方提取业务域
            'table_comment': table_desc,
            'file_path': file_path,
            'create_time': now,
            'modify_time': now
        })
        
        logger.debug(f"[metadataService] 收集 table_metadata: {table_metadata_id} - {target_table} ({layer_type})")
