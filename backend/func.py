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

