from typing import List

from config import *
from utils import *


def setup_environment():
    create_table_result = create_hive_table(
        table_name='car_data',
        schema=car_data_schema,
        config=HIVE_CONFIG
    )
    print(create_table_result)


def insert_data(car_data: List[dict]):
    insert_result = insert_into_hive_table(
        table_name='car_data',
        data=car_data,
        schema=car_data_schema,  # 传入 schema 以便处理复杂类型
        config=HIVE_CONFIG
    )
    print(insert_result)


def read_data(filters):
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
    output = read_from_hive_table(
        table_name='car_data',
        config=HIVE_CONFIG,
        filters=filters
    )
    return output['data']