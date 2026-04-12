from fastapi import APIRouter

from apps.chat.api import chat
from apps.dashboard.api import dashboard_api
from apps.data_training.api import data_training
from apps.datasource.api import datasource, table_relation, recommended_problem
from apps.mcp import mcp
from apps.system.api import login, user, aimodel, workspace, assistant
from apps.terminology.api import terminology
from apps.settings.api import base

# metrics2 模块路由
from apps.extend.metrics2 import (
    dim_dict_router,
    metric_definition_router,
    metric_dim_rel_router,
    metric_source_mapping_router,
    metric_compound_rel_router,
    version_management_router,
    lineage_analysis_router
)

api_router = APIRouter()
api_router.include_router(login.router)
api_router.include_router(user.router)
api_router.include_router(workspace.router)
api_router.include_router(assistant.router)
api_router.include_router(aimodel.router)
api_router.include_router(base.router)
api_router.include_router(terminology.router)
api_router.include_router(data_training.router)
api_router.include_router(datasource.router)
api_router.include_router(chat.router)
api_router.include_router(dashboard_api.router)
api_router.include_router(mcp.router)
api_router.include_router(table_relation.router)

api_router.include_router(recommended_problem.router)

# 注册 metrics2 模块路由
api_router.include_router(dim_dict_router)
api_router.include_router(metric_definition_router)
api_router.include_router(metric_dim_rel_router)
api_router.include_router(metric_source_mapping_router)
api_router.include_router(metric_compound_rel_router)
api_router.include_router(version_management_router)
api_router.include_router(lineage_analysis_router)
