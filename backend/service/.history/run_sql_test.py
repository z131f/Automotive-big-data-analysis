from impala.dbapi import connect
from config import HIVE_CONFIG

config = HIVE_CONFIG

conn = connect(**config)
cursor = conn.cursor()

print('-------------------drop table')

cursor.execute('DROP TABLE IF EXISTS car_data')

print('-------------------create table')

cursor.execute('''CREATE TABLE IF NOT EXISTS car_data (
    car_brand STRING, city STRING, car_model STRING, manufacturer_suggested_price DECIMAL(10, 2), engine_horsepower INT, num_doors INT, min_reference_price DECIMAL(10, 2), car_type STRING, manufacture_year INT, fuel_capacity DECIMAL(5, 2), popularity INT, discount_percentage DECIMAL(5, 2), historical_price MAP<STRING, INT>, city_license_plates MAP<STRING, INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
''')

print('-------------------insert data')

cursor.execute('''INSERT INTO TABLE car_data VALUES
('丰田', '北京', '凯美瑞', 180000.0, 178, 4, 175000.0, '轿车', 2023, 60.0, 85, 2.5, 
 map('2025.7', 180000, '2025.6', 179000, '2025.5', 178500),  -- 使用map函数
 map('北京', 1200, '上海', 1000)),  -- 移除花括号和嵌套引号

('特斯拉', '上海', 'Model 3', 250000.0, 275, 4, 245000.0, '电动汽车', 2024, 0.0, 95, 1.0, 
 map('2025.8', 250000, '2024.6', 248000), 
 map('上海', 1500, '深圳', 800)),

-- 第三行保留（但注意num_doors=400可能是数据异常）
('特斯拉', '上海', 'Model 3', 250000.0, 275, 400, 245000.0, '电动汽车', 2024, 0.0, 95, 1.0, 
 map('2025.8', 250000, '2024.6', 248000), 
 map('上海', 1500, '深圳', 800))
''')

print('-------------------read data')

cursor.execute("SELECT car_type FROM default.car_data WHERE city = '上海';")


