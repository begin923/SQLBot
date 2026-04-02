"""SQL 校验器测试用例"""
from apps.extend.utils.check import SQLValidator


def test_is_ads_dws_layer():
    """测试 ADS/DWS 层识别"""
    
    # 应该返回 True 的情况
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM yz_datawarehouse_ads.table1") == True
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM yz_datawarehouse_dws.table1") == True
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM ads_fpf_female_prod_day") == True
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM dws_user_behavior") == True
    assert SQLValidator.is_ads_dws_layer("SELECT a.* FROM ADS_TABLE a") == True
    assert SQLValidator.is_ads_dws_layer("SELECT d.* FROM DWS_TABLE d") == True
    
    # 应该返回 False 的情况
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM yz_datawarehouse_dwd.table1") == False
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM yz_datawarehouse_ods.table1") == False
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM per_user") == False
    assert SQLValidator.is_ads_dws_layer("SELECT * FROM users") == False
    
    print("✅ test_is_ads_dws_layer 通过")


def test_has_group_by():
    """测试 GROUP BY 检测"""
    
    # 应该返回 True
    assert SQLValidator.has_group_by("SELECT dt_month, SUM(amount) FROM table GROUP BY dt_month") == True
    assert SQLValidator.has_group_by("SELECT dt_month, SUM(amount) FROM table group by dt_month") == True
    assert SQLValidator.has_group_by("SELECT dt_month, SUM(amount) FROM table Group By dt_month") == True
    assert SQLValidator.has_group_by("SELECT dt_month, SUM(amount) FROM table GROUP   BY dt_month") == True
    
    # 应该返回 False
    assert SQLValidator.has_group_by("SELECT * FROM table") == False
    assert SQLValidator.has_group_by("SELECT dt_month, amount FROM table") == False
    assert SQLValidator.has_group_by("") == False
    
    print("✅ test_has_group_by 通过")


def test_has_aggregate_function():
    """测试聚合函数检测"""
    
    # 应该返回 True
    assert SQLValidator.has_aggregate_function("SELECT SUM(amount) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT COUNT(*) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT AVG(price) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT MAX(value) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT MIN(value) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT sum(amount) FROM table") == True
    assert SQLValidator.has_aggregate_function("SELECT Sum(amount) FROM table") == True
    
    # 应该返回 False
    assert SQLValidator.has_aggregate_function("SELECT amount FROM table") == False
    assert SQLValidator.has_aggregate_function("SELECT * FROM table") == False
    assert SQLValidator.has_aggregate_function("") == False
    
    print("✅ test_has_aggregate_function 通过")


def test_validate_ads_dws_sql_valid():
    """测试有效的 ADS/DWS SQL"""
    
    # 完整的聚合查询
    sql = "SELECT dt_month, SUM(d7_sum) FROM yz_datawarehouse_ads.ads_fpf_female_prod_day GROUP BY dt_month"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == True
    assert error_msg is None
    
    # 多个聚合函数
    sql = "SELECT dt_month, SUM(d7_sum), COUNT(*) FROM yz_datawarehouse_dws.dws_user_behavior GROUP BY dt_month"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == True
    assert error_msg is None
    
    print("✅ test_validate_ads_dws_sql_valid 通过")


def test_validate_ads_dws_sql_invalid():
    """测试无效的 ADS/DWS SQL（缺少聚合）"""
    
    # 没有 GROUP BY 和聚合函数
    sql = "SELECT dt_date, d7_sum FROM yz_datawarehouse_ads.ads_fpf_female_prod_day"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == False
    assert error_msg is not None
    assert "缺少 GROUP BY 和聚合函数" in error_msg
    
    # 有聚合函数但没有 GROUP BY
    sql = "SELECT SUM(d7_sum) FROM yz_datawarehouse_ads.ads_fpf_female_prod_day"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == False
    assert error_msg is not None
    assert "缺少 GROUP BY" in error_msg
    
    # 有 GROUP BY 但没有聚合函数
    sql = "SELECT dt_month, d7_sum FROM yz_datawarehouse_ads.ads_fpf_female_prod_day GROUP BY dt_month"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == False
    assert error_msg is not None
    assert "缺少聚合函数" in error_msg
    
    print("✅ test_validate_ads_dws_sql_invalid 通过")


def test_validate_non_ads_dws_sql():
    """测试非 ADS/DWS 层 SQL（应该直接通过）"""
    
    # DWD 层查询（允许不聚合）
    sql = "SELECT dt_date, feed_count FROM yz_datawarehouse_dwd.dwd_feed_detail"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == True
    assert error_msg is None
    
    # ODS 层查询（允许不聚合）
    sql = "SELECT user_id, action FROM ods_user_behavior"
    is_valid, error_msg = SQLValidator.validate_ads_dws_sql(sql)
    assert is_valid == True
    assert error_msg is None
    
    print("✅ test_validate_non_ads_dws_sql 通过")


if __name__ == "__main__":
    test_is_ads_dws_layer()
    test_has_group_by()
    test_has_aggregate_function()
    test_validate_ads_dws_sql_valid()
    test_validate_ads_dws_sql_invalid()
    test_validate_non_ads_dws_sql()
    
    print("\n🎉 所有测试通过！")
