HIVE_CONFIG = {
    "host": "your_hiveserver2_host",  # 例如: "localhost" 或 HiveServer2 所在服务器的 IP
    "port": 10000,                  # HiveServer2 默认端口通常是 10000
    "auth_mechanism": "PLAIN",      # 认证机制，根据您的 Hive 配置选择 (PLAIN, KERBEROS, LDAP, NOSASL 等)
    "database": "default"           # 默认数据库
}

car_data_schema = {
    'car_brand': 'STRING',
    'city': 'STRING',
    'car_model': 'STRING',
    'manufacturer_suggested_price': 'DECIMAL(10, 2)',
    'engine_horsepower': 'INT',
    'num_doors': 'INT',
    'min_reference_price': 'DECIMAL(10, 2)',
    'car_type': 'STRING',
    'manufacture_year': 'INT',
    'fuel_capacity': 'DECIMAL(5, 2)',
    'popularity': 'INT',
    'discount_percentage': 'DECIMAL(5, 2)',
    'historical_price': 'MAP<STRING, INT>', # 注意 ARRAY 类型
    'city_license_plates': 'MAP<STRING, INT>',   # 注意 MAP 类型
}