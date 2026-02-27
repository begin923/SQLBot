# 执行传入动态参数的数据集sql
请你执行当前的sql并输出对应的数据查询结果#sql#
{
"sql":"select id
where dt_date = '${dtDate}'
  and t2.parent_id = ${orgId}
",
"in_parm":{
	"orgId":626804036418404352,
	"dtDate":"2026-02-24"
	}
}#sql#

