# from typing import List
# import random
# from config import *
# from utils import *
#
#
# def setup_environment():
#     create_table_result = create_hive_table(
#         table_name='car_data',
#         schema=car_data_schema,
#         config=HIVE_CONFIG
#     )
#     print(create_table_result)
#
#
# def insert_data(car_data):
#     insert_result = insert_into_hive_table(
#         table_name='car_data',
#         data=car_data,
#         schema=car_data_schema,  # 传入 schema 以便处理复杂类型
#         config=HIVE_CONFIG
#     )
#     print(insert_result)
#
#
# def read_data_with_filters(filters=None, name='*', is_distinct=False):
#     """
#     filters: 筛选条件
#     example:
#     data = read_data(
#         filters={
#             'city': '成都',
#             'num_doors': 4,
#         }
#     )
#     """
#     if is_distinct:
#         assert name != '*'
#         name = f'DISTINCT {name}'
#     output = read_from_hive_table(
#         table_name='car_data',
#         config=HIVE_CONFIG,
#         filters=filters,
#         name=name
#     )
#     return output['data']
#
#
# def rand_data_generate(num_records):
#     """
#     根据给定的数据结构模式生成随机数据。
#
#     Args:
#         schema (dict): 定义数据字段及其类型的字典。
#                        支持的类型包括 'STRING', 'INT', 'DECIMAL(p, s)',
#                        'MAP<STRING, INT>'。
#         num_records (int): 要生成的记录数量。
#
#     Returns:
#         list: 包含生成的随机数据字典的列表。
#     """
#     schema = car_data_schema
#     generated_data = []
#
#     # 预定义的模拟数据池，确保数据多样性
#     car_brands = ["丰田", "本田", "大众", "奔驰", "宝马", "奥迪", "特斯拉", "比亚迪"]
#     cities = ["北京", "上海", "广州", "深圳", "成都", "杭州", "重庆", "武汉"]
#     car_models = {
#         "丰田": ["凯美瑞", "卡罗拉", "RAV4荣放"],
#         "本田": ["雅阁", "思域", "CR-V"],
#         "大众": ["朗逸", "速腾", "迈腾"],
#         "奔驰": ["C级", "E级", "S级"],
#         "宝马": ["3系", "5系", "X5"],
#         "奥迪": ["A4L", "A6L", "Q5L"],
#         "特斯拉": ["Model 3", "Model Y"],
#         "比亚迪": ["汉", "宋", "秦"],
#     }
#     car_types = ["轿车", "SUV", "MPV", "跑车", "皮卡"]
#
#     for _ in range(num_records):
#         record = {}
#         # 随机选择一个品牌，确保模型与品牌匹配
#         brand = random.choice(car_brands)
#         model = random.choice(car_models.get(brand, ["未知车型"])) # 如果品牌没有匹配的模型，则使用“未知车型”
#
#         for field, data_type in schema.items():
#             if field == 'car_brand':
#                 record[field] = brand
#             elif field == 'car_model':
#                 record[field] = model
#             elif field == 'city':
#                 record[field] = random.choice(cities)
#             elif data_type == 'STRING':
#                 # 为其他字符串类型生成随机字符串或从预定义列表中选择
#                 if field == 'car_type':
#                     record[field] = random.choice(car_types)
#                 else:
#                     record[field] = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=random.randint(5, 10)))
#             elif data_type.startswith('DECIMAL'):
#                 # 修正：现在生成的是 float 类型，不再进行 Decimal 的精确量化
#                 if field == 'manufacturer_suggested_price':
#                     record[field] = round(random.uniform(80000, 500000), 2) # 保留两位小数
#                 elif field == 'min_reference_price':
#                     # 确保参考价格低于或等于建议价格
#                     record[field] = round(random.uniform(0.8 * record['manufacturer_suggested_price'], record['manufacturer_suggested_price']), 2)
#                 elif field == 'fuel_capacity':
#                     record[field] = round(random.uniform(30, 80), 2)
#                 elif field == 'discount_percentage':
#                     record[field] = round(random.uniform(0, 20), 2)
#                 else:
#                     # 对于其他DECIMAL，生成通用浮点数
#                     record[field] = round(random.uniform(0, 10000), 2) # 默认保留两位小数
#             elif data_type == 'INT':
#                 if field == 'engine_horsepower':
#                     record[field] = random.randint(80, 500)
#                 elif field == 'num_doors':
#                     record[field] = random.choice([2, 4, 5]) # 常见门数
#                 elif field == 'manufacture_year':
#                     record[field] = random.randint(2010, 2025)
#                 elif field == 'popularity':
#                     record[field] = random.randint(1, 1000)
#                 else:
#                     record[field] = random.randint(0, 10000)
#             elif data_type == 'MAP<STRING, INT>':
#                 if field == 'historical_price':
#                     # 生成随机的历史价格，键可以是年份或月份
#                     num_entries = random.randint(3, 7)
#                     history = {}
#                     current_year = 2025
#                     for i in range(num_entries):
#                         year_month = f"{current_year - i}-{random.randint(1, 12):02d}"
#                         history[year_month] = random.randint(50000, 400000)
#                     record[field] = history
#                 elif field == 'city_license_plates':
#                     # 生成随机的城市车牌数量，键是城市
#                     num_entries = random.randint(2, 5)
#                     license_plates = {}
#                     selected_cities = random.sample(cities, min(num_entries, len(cities)))
#                     for city_name in selected_cities:
#                         license_plates[city_name] = random.randint(1000, 100000)
#                     record[field] = license_plates
#                 else:
#                     # 默认的 MAP<STRING, INT> 生成逻辑
#                     map_data = {}
#                     num_entries = random.randint(1, 5)
#                     for _ in range(num_entries):
#                         key = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
#                         value = random.randint(1, 100)
#                         map_data[key] = value
#                     record[field] = map_data
#             else:
#                 # 如果遇到不支持的类型，可以返回None或抛出错误
#                 record[field] = None
#         generated_data.append(record)
#     return generated_data


from typing import List
import random
from config import *
from utils import *


def setup_environment():
    create_table_result = create_hive_table(
        table_name='car_data',
        schema=car_data_schema,
        config=HIVE_CONFIG
    )
    print(create_table_result)


def insert_data(car_data):
    insert_result = insert_into_hive_table(
        table_name='car_data',
        data=car_data,
        schema=car_data_schema,  # 传入 schema 以便处理复杂类型
        config=HIVE_CONFIG
    )
    print(insert_result)


def read_data_with_filters(filters=None, name='*', is_distinct=False):
    """
    filters: 筛选条件
    example:
    data = read_data(
        filters={
            'city': '成都',
            'num_doors': 4,
        }
    )
    """
    if is_distinct:
        assert name != '*'
        name = f'DISTINCT {name}'
    output = read_from_hive_table(
        table_name='car_data',
        config=HIVE_CONFIG,
        filters=filters,
        name=name
    )
    # Return the entire output dictionary, including status and message
    return output


def rand_data_generate(num_records):
    """
    生成指定数量的随机汽车数据记录。
    """
    brands = ['丰田', '本田', '大众', '奥迪', '宝马', '奔驰', '特斯拉']
    models = ['卡罗拉', '思域', '帕萨特', 'A4', '3系', 'C级', 'Model 3']
    car_types = ['轿车', 'SUV', 'MPV', '跑车']
    cities = ['北京', '上海', '广州', '深圳', '成都', '杭州', '重庆']

    data = []
    for i in range(num_records):
        record = {
            'car_brand': random.choice(brands),
            'city': random.choice(cities),
            'car_model': random.choice(models),
            'manufacturer_suggested_price': round(random.uniform(100000, 500000), 2),
            'engine_horsepower': random.randint(100, 400),
            'num_doors': random.choice([2, 4, 5]),
            'min_reference_price': round(random.uniform(90000, 480000), 2),
            'car_type': random.choice(car_types),
            'manufacture_year': random.randint(2015, 2024),
            'fuel_capacity': round(random.uniform(40.0, 80.0), 2),
            'popularity': random.randint(100, 1000),
            'discount_percentage': round(random.uniform(0.5, 15.0), 2),
        }

        # Add historical_price and city_license_plates as Python dictionaries
        # These will be formatted into map() literal by utils.py

        # 生成随机的历史价格，键可以是年份或月份
        num_entries_hp = random.randint(3, 7)
        history = {}
        current_year = 2025
        for j in range(num_entries_hp):
            year_month = f"{current_year - j}-{random.randint(1, 12):02d}"
            history[year_month] = random.randint(50000, 400000)
        record['historical_price'] = history

        # 生成随机的城市车牌数量，键是城市
        num_entries_clp = random.randint(2, 5)
        license_plates = {}
        selected_cities = random.sample(cities, min(num_entries_clp, len(cities)))
        for city_name in selected_cities:
            license_plates[city_name] = random.randint(1000, 100000)
        record['city_license_plates'] = license_plates

        data.append(record)
    return data

