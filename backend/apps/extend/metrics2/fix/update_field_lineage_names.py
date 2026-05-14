"""
批量更新 field_lineage 表中的中文名称字段
分批处理，每批500条记录
"""
import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

# 添加项目根目录到 Python 路径
from apps.extend.utils.utils import DBUtils, ModelClient

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_field_names.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FieldNameUpdater:
    """字段名称更新器"""
    
    def __init__(self):
        self.model_client = ModelClient()
        self.batch_size = 50  # 批量处理，每批 50 条记录（避免单次请求过大）
        
    def get_records_without_names(self, session, offset: int = 0, limit: int = 500) -> List[Dict]:
        """
        查询缺少中文名称的记录
        
        Args:
            session: 数据库会话
            offset: 偏移量
            limit: 限制数量
            
        Returns:
            记录列表
        """
        # 查询 source_table_name, target_table_name, source_field_name, target_field_name 为空的记录
        query = text("""
            SELECT 
                id,
                source_table,
                target_table,
                source_field,
                target_field
            FROM field_lineage
            WHERE source_table_name IS NULL 
               OR target_table_name IS NULL 
               OR source_field_name IS NULL 
               OR target_field_name IS NULL
            ORDER BY id
            LIMIT :limit OFFSET :offset
        """)
        
        result = session.execute(query, {"limit": limit, "offset": offset})
        records = []
        
        for row in result:
            records.append({
                'id': row[0],
                'source_table': row[1],
                'target_table': row[2],
                'source_field': row[3],
                'target_field': row[4]
            })
            
        return records
    
    def generate_chinese_names_batch(self, records: List[Dict]) -> Optional[List[Dict]]:
        """
        批量调用大模型生成中文名称
        
        Args:
            records: 记录列表，每条记录包含 id, source_table, target_table, source_field, target_field
            
        Returns:
            包含中文名称的字典列表，失败返回 None
        """
        try:
            # 构建批量输入数据
            input_records = []
            for record in records:
                input_records.append({
                    "id": record['id'],
                    "source_table": record['source_table'],
                    "target_table": record['target_table'],
                    "source_field": record['source_field'],
                    "target_field": record['target_field']
                })
            
            # 将记录列表转换为 JSON 字符串
            print(f"输入数据: {input_records}")
            import json
            records_json = json.dumps(input_records, ensure_ascii=False, indent=2)
            
            # 调用 AI 模型
            response = self.model_client.call_ai(
                template_name="field_name_translation",
                sql_content=records_json
            )
            
            if not response:
                logger.warning("AI 返回空响应")
                return None
                
            # 解析 JSON 响应
            try:
                # 尝试提取 JSON 部分（如果响应包含 markdown 代码块）
                json_str = response
                if '```json' in response:
                    json_str = response.split('```json')[1].split('```')[0].strip()
                elif '```' in response:
                    json_str = response.split('```')[1].split('```')[0].strip()
                    
                result = json.loads(json_str)
                
                if result.get('code') == 0 and 'data' in result:
                    data_list = result['data']
                    
                    # 验证返回数量是否与输入一致
                    if len(data_list) != len(records):
                        logger.warning(f"AI 返回数量不匹配: 期望 {len(records)}, 实际 {len(data_list)}")
                        return None
                    
                    return data_list
                else:
                    logger.warning(f"AI 返回错误: {result.get('msg', '未知错误')}")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON 解析失败: {e}, 响应内容: {response[:200]}")
                return None
                
        except Exception as e:
            logger.error(f"调用 AI 模型失败: {e}")
            return None
    
    def update_record_names_batch(self, session, results: List[Dict]):
        """
        批量更新记录的中文名称
        
        Args:
            session: 数据库会话
            results: AI 返回的结果列表，每个元素包含 id 和四个中文字段
        """
        for result in results:
            record_id = result.get('id')
            if not record_id:
                logger.warning("结果中缺少 id 字段，跳过")
                continue
                
            update_query = text("""
                UPDATE field_lineage
                SET 
                    source_table_name = :source_table_name,
                    target_table_name = :target_table_name,
                    source_field_name = :source_field_name,
                    target_field_name = :target_field_name,
                    modify_time = NOW()
                WHERE id = :record_id
            """)
            
            session.execute(update_query, {
                "record_id": record_id,
                "source_table_name": result.get('source_table_name'),
                "target_table_name": result.get('target_table_name'),
                "source_field_name": result.get('source_field_name'),
                "target_field_name": result.get('target_field_name')
            })
        
    def process_batch(self, session, records: List[Dict]) -> Dict[str, int]:
        """
        处理一批记录（批量调用 AI）
        
        Args:
            session: 数据库会话
            records: 记录列表
            
        Returns:
            处理结果统计
        """
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        try:
            logger.info(f"开始批量处理 {len(records)} 条记录...")
            
            # 批量调用 AI 生成中文名称
            results = self.generate_chinese_names_batch(records)
            
            if not results:
                logger.error(f"❌ 批量生成中文名称失败，本批 {len(records)} 条记录全部失败")
                return {
                    'success': 0,
                    'fail': len(records),
                    'skip': 0
                }
            
            # 批量更新数据库
            self.update_record_names_batch(session, results)
            success_count = len(results)
            
            logger.info(f"✅ 成功更新 {success_count} 条记录")
            
        except Exception as e:
            fail_count = len(records)
            logger.error(f"❌ 处理批次时出错: {e}")
            
        return {
            'success': success_count,
            'fail': fail_count,
            'skip': skip_count
        }
    
    def run(self):
        """运行批量更新任务"""
        logger.info("开始批量更新 field_lineage 表中的中文名称")
        
        # 创建数据库会话
        session = DBUtils.create_local_session(echo_sql=False)
        
        try:
            offset = 0
            total_processed = 0
            total_success = 0
            total_fail = 0
            
            while True:
                # 获取一批记录
                records = self.get_records_without_names(session, offset, self.batch_size)
                
                if not records:
                    logger.info("没有更多记录需要处理")
                    break
                    
                logger.info(f"处理第 {offset // self.batch_size + 1} 批，共 {len(records)} 条记录")
                
                # 处理这批记录
                result = self.process_batch(session, records)
                
                total_processed += len(records)
                total_success += result['success']
                total_fail += result['fail']
                
                logger.info(f"批次处理结果: 成功 {result['success']}, 失败 {result['fail']}, 跳过 {result['skip']}")
                
                # 提交事务
                session.commit()
                
                # 移动到下一批
                offset += self.batch_size
                
                # 添加延迟，避免过于频繁的数据库操作
                import time
                time.sleep(1)
                
            logger.info(f"批量更新完成! 总共处理: {total_processed}, 成功: {total_success}, 失败: {total_fail}")
            
        except Exception as e:
            logger.error(f"批量更新过程中出错: {e}")
            session.rollback()
            raise
        finally:
            session.close()


if __name__ == "__main__":
    updater = FieldNameUpdater()
    updater.run()