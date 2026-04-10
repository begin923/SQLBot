from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from apps.extend.metrics2.curd import (
    create_dim_dict,
    batch_create_dim_dict,
    update_dim_dict,
    delete_dim_dict,
    get_dim_dict_by_id,
    get_dim_dict_by_code
)
from apps.extend.metrics2.models import DimDictInfo
import logging

logger = logging.getLogger("DimFieldMappingService")


class DimFieldMappingService:
    """维度定义服务类 - 负责维度相关的业务逻辑编排"""

    def __init__(self, session: Session):
        """
        初始化维度字典服务

        Args:
            session: 数据库会话
        """
        self.session = session

    def create_dim_dict_with_validation(self, dim_info: DimDictInfo) -> Dict[str, Any]:
        """
        创建维度定义并验证唯一性

        Args:
            dim_info: 维度定义信息

        Returns:
            创建结果字典
        """
        try:
            # 验证维度编码唯一性
            existing_dim = get_dim_dict_by_code(self.session, dim_info.dim_code)
            if existing_dim:
                return {
                    "success": False,
                    "message": f"维度编码 {dim_info.dim_code} 已存在",
                    "existing_dim": existing_dim
                }

            # 创建维度定义
            dim_id = create_dim_dict(self.session, dim_info)
            logger.info(f"成功创建维度定义：{dim_id}")

            return {
                "success": True,
                "dim_id": dim_id,
                "message": "维度定义创建成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"创建维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"创建失败：{str(e)}",
                "dim_code": dim_info.dim_code if hasattr(dim_info, 'dim_code') else None
            }

    def batch_create_dim_dict_with_validation(self, dim_infos: List[DimDictInfo]) -> Dict[str, Any]:
        """
        批量创建维度定义并验证唯一性

        Args:
            dim_infos: 维度定义信息列表

        Returns:
            批量创建结果
        """
        try:
            # 验证所有维度编码的唯一性
            dim_codes = [info.dim_code for info in dim_infos if info.dim_code]
            existing_dims = get_dim_dict_by_code(self.session, dim_codes)

            if existing_dims:
                return {
                    "success": False,
                    "message": "发现重复的维度编码",
                    "duplicate_codes": list(set(existing_dims.keys())),
                    "existing_dims": existing_dims
                }

            # 批量创建维度定义
            result = batch_create_dim_dict(self.session, dim_infos)
            logger.info(f"批量创建维度定义完成，成功：{result['success_count']}，失败：{len(result['failed_records'])}")

            return {
                "success": True,
                "message": "批量创建维度定义成功",
                "total_processed": result['original_count'],
                "success_count": result['success_count'],
                "failed_count": result['duplicate_count'] + len(result['failed_records']),
                "batch_result": result
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"批量创建维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"批量创建失败：{str(e)}",
                "total_processed": len(dim_infos) if 'dim_infos' in locals() else 0
            }

    def update_dim_dict_with_validation(self, dim_info: DimDictInfo) -> Dict[str, Any]:
        """
        更新维度定义并验证

        Args:
            dim_info: 维度定义信息

        Returns:
            更新结果字典
        """
        try:
            # 验证维度ID存在
            existing_dim = get_dim_dict_by_id(self.session, dim_info.dim_id)
            if not existing_dim:
                return {
                    "success": False,
                    "message": f"维度ID {dim_info.dim_id} 不存在"
                }

            # 验证维度编码唯一性（排除自身）
            if dim_info.dim_code:
                existing_dim_by_code = get_dim_dict_by_code(self.session, dim_info.dim_code)
                if existing_dim_by_code and existing_dim_by_code.dim_id != dim_info.dim_id:
                    return {
                        "success": False,
                        "message": f"维度编码 {dim_info.dim_code} 已被其他维度使用"
                    }

            # 更新维度定义
            update_dim_dict(self.session, dim_info)
            logger.info(f"成功更新维度定义：{dim_info.dim_id}")

            return {
                "success": True,
                "dim_id": dim_info.dim_id,
                "message": "维度定义更新成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"更新维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"更新失败：{str(e)}",
                "dim_id": dim_info.dim_id
            }

    def get_dim_dict_with_relations(self, dim_id: str) -> Dict[str, Any]:
        """
        获取维度定义及其关联信息

        Args:
            dim_id: 维度ID

        Returns:
            包含维度及其关联信息的字典
        """
        try:
            # 获取维度定义
            dim_dict = get_dim_dict_by_id(self.session, dim_id)
            if not dim_dict:
                return {"success": False, "message": "维度不存在"}

            # 这里可以添加获取维度关联信息的逻辑
            # 例如：获取使用该维度的指标列表

            return {
                "success": True,
                "dim_dict": dim_dict,
                "message": "获取维度定义成功"
            }

        except Exception as e:
            logger.error(f"获取维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"获取失败：{str(e)}",
                "dim_id": dim_id
            }

    def delete_dim_dict_with_checks(self, dim_id: str) -> Dict[str, Any]:
        """
        删除维度定义并进行检查

        Args:
            dim_id: 维度ID

        Returns:
            删除结果字典
        """
        try:
            # 检查维度是否被指标使用
            # 这里需要实现检查逻辑，例如查询metric_dim_rel表
            # 假设有检查逻辑，如果被使用则不允许删除
            is_in_use = self._check_dim_in_use(dim_id)
            if is_in_use:
                return {
                    "success": False,
                    "message": f"维度 {dim_id} 正在被指标使用，无法删除"
                }

            # 删除维度定义
            delete_dim_dict(self.session, [dim_id])
            logger.info(f"成功删除维度定义：{dim_id}")

            return {
                "success": True,
                "dim_id": dim_id,
                "message": "维度定义删除成功"
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"删除维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"删除失败：{str(e)}",
                "dim_id": dim_id
            }

    def _check_dim_in_use(self, dim_id: str) -> bool:
        """
        检查维度是否被指标使用（私有方法）

        Args:
            dim_id: 维度ID

        Returns:
            是否被使用
        """
        # 这里需要实现实际的检查逻辑
        # 例如：查询metric_dim_rel表，检查该维度ID是否存在
        # 假设检查逻辑，返回True表示被使用，False表示未被使用
        # 实际实现需要根据具体业务逻辑调整
        return False  # 暂时返回False，实际需要实现检查逻辑

    def sync_dim_dict_from_source(self, source_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        从外部数据源同步维度定义

        Args:
            source_data: 外部数据源中的维度数据列表

        Returns:
            同步结果
        """
        try:
            # 验证输入数据
            if not source_data or len(source_data) == 0:
                return {"success": False, "message": "源数据不能为空"}

            # 准备维度定义信息
            dim_infos = []
            for data in source_data:
                dim_infos.append(DimDictInfo(
                    dim_name=data.get('dim_name'),
                    dim_code=data.get('dim_code'),
                    dim_type=data.get('dim_type', '普通维度'),
                    dim_table=data.get('dim_table'),
                    dim_column=data.get('dim_column'),
                    is_valid=data.get('is_valid', True)
                ))

            # 批量创建或更新维度定义
            result = self.batch_create_dim_dict_with_validation(dim_infos)

            return {
                "success": result.get('success', False),
                "message": result.get('message', '未知错误'),
                "total_processed": len(source_data),
                "created_count": result.get('success_count', 0),
                "sync_result": result
            }

        except Exception as e:
            logger.error(f"同步维度定义失败：{str(e)}")
            return {
                "success": False,
                "message": f"同步失败：{str(e)}",
                "source_count": len(source_data) if 'source_data' in locals() else 0
            }