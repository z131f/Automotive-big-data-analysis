from config import HIVE_CONFIG, car_data_schema
from utils import create_hive_table, insert_into_hive_table, read_from_hive_table
from func import setup_environment, insert_data, read_data_with_filters

setup_environment()
sample_data = [
    {
        'car_brand': '丰田',
        'city': '北京',
        'car_model': '凯美瑞',
        'manufacturer_suggested_price': 180000.00,
        'engine_horsepower': 178,
        'num_doors': 4,
        'min_reference_price': 175000.00,
        'car_type': '轿车',
        'manufacture_year': 2023,
        'fuel_capacity': 60.0,
        'popularity': 85,
        'discount_percentage': 2.5,
        'historical_price': {'2025.7': 180000.00, '2025.6': 179000.00, '2025.5': 178500.00}, # 列表
        'city_license_plates': {'北京': 1200, '上海': 1000}, # 字典
    },
    {
        'car_brand': '特斯拉',
        'city': '上海',
        'car_model': 'Model 3',
        'manufacturer_suggested_price': 250000.00,
        'engine_horsepower': 275,
        'num_doors': 4,
        'min_reference_price': 245000.00,
        'car_type': '电动汽车',
        'manufacture_year': 2024,
        'fuel_capacity': 0.0, # 电动车油量为0
        'popularity': 95,
        'discount_percentage': 1.0,
        'historical_price': {'2025.8': 250000.00, '2024.6': 248000.00},
        'city_license_plates': {'上海': 1500, '深圳': 800},
    },
{
        'car_brand': '特斯拉',
        'city': '上海',
        'car_model': 'Model 3',
        'manufacturer_suggested_price': 250000.00,
        'engine_horsepower': 275,
        'num_doors': 400,
        'min_reference_price': 245000.00,
        'car_type': '电动汽车',
        'manufacture_year': 2024,
        'fuel_capacity': 0.0, # 电动车油量为0
        'popularity': 95,
        'discount_percentage': 1.0,
        'historical_price': {'2025.8': 250000.00, '2024.6': 248000.00},
        'city_license_plates': {'上海': 1500, '深圳': 800},
    }
]

insert_data(sample_data)

print(read_data_with_filters(filters=None, name='city', is_distinct=True))
print(read_data_with_filters(filters=None, name='city', is_distinct=False))
print(read_data_with_filters(filters={'num_doors': 4}, name='city', is_distinct=False))
print(read_data_with_filters(filters={'num_doors': 4}, is_distinct=False))



# # 1. 创建表
# create_table_result = create_hive_table(
#     table_name='car_data',
#     schema=car_data_schema,
#     config=HIVE_CONFIG
# )
# print(create_table_result)
#
# # 2. 准备数据并插入
# # 注意：确保 historical_price 是列表，city_license_plates 是字典
# sample_data = [
#     {
#         'car_brand': '丰田',
#         'city': '北京',
#         'car_model': '凯美瑞',
#         'manufacturer_suggested_price': 180000.00,
#         'engine_horsepower': 178,
#         'num_doors': 4,
#         'min_reference_price': 175000.00,
#         'car_type': '轿车',
#         'manufacture_year': 2023,
#         'fuel_capacity': 60.0,
#         'popularity': 85,
#         'discount_percentage': 2.5,
#         'historical_price': [180000.00, 179000.00, 178500.00], # 列表
#         'city_license_plates': {'北京': 1200, '上海': 1000}, # 字典
#     },
#     {
#         'car_brand': '特斯拉',
#         'city': '上海',
#         'car_model': 'Model 3',
#         'manufacturer_suggested_price': 250000.00,
#         'engine_horsepower': 275,
#         'num_doors': 4,
#         'min_reference_price': 245000.00,
#         'car_type': '电动汽车',
#         'manufacture_year': 2024,
#         'fuel_capacity': 0.0, # 电动车油量为0
#         'popularity': 95,
#         'discount_percentage': 1.0,
#         'historical_price': [250000.00, 248000.00],
#         'city_license_plates': {'上海': 1500, '深圳': 800},
#     }
# ]
#
# insert_result = insert_into_hive_table(
#     table_name='car_data',
#     data=sample_data,
#     schema=car_data_schema, # 传入 schema 以便处理复杂类型
#     config=HIVE_CONFIG
# )
# print(insert_result)
#
# # 3. 读取数据
# read_result = read_from_hive_table(
#     table_name='car_data',
#     filters={'car_brand': '丰田'},
#     config=HIVE_CONFIG
# )
# print(read_result)