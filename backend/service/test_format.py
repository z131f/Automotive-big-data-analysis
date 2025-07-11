from config import car_data_schema, HIVE_CONFIG

config = HIVE_CONFIG
table_name = 'car_data'
schema = car_data_schema
columns_sql = []
for col_name, col_type in schema.items():
    columns_sql.append(f"{col_name} {col_type}")

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {', '.join(columns_sql)}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
"""

print(create_table_sql)


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

data = sample_data


columns = list(schema.keys())

all_rows_values = []
for row_dict in data:
    row_values_formatted = []
    for col_name in columns:
        value = row_dict.get(col_name)
        hive_type = schema.get(col_name, 'STRING').upper()

        if value is None:
            row_values_formatted.append("NULL")
        elif 'ARRAY' in hive_type and isinstance(value, list):
            # 直接拼接数组元素，不进行转义
            formatted_items = [str(item) for item in value]
            row_values_formatted.append(f"'[{','.join(formatted_items)}]'")
        elif 'MAP' in hive_type and isinstance(value, dict):
            # 直接拼接Map键值对，不进行转义
            formatted_items = []
            for k, v in value.items():
                # 对于字符串键值，加上单引号
                formatted_k = f"'{k}'" if isinstance(k, str) else str(k)
                formatted_v = f"'{v}'" if isinstance(v, str) else str(v)
                formatted_items.append(f"{formatted_k}:{formatted_v}")
            row_values_formatted.append(f"'{'{'}{','.join(formatted_items)}{'}'}'")
        elif isinstance(value, str):
            # 对于普通字符串，直接用单引号包裹，不进行内部转义
            row_values_formatted.append(f"'{value}'")
        else:
            row_values_formatted.append(str(value))

    all_rows_values.append(f"({', '.join(row_values_formatted)})")

insert_sql = f"INSERT INTO TABLE {table_name} VALUES {', '.join(all_rows_values)}"

print(insert_sql)

filters = {'city': '上海'}
name = 'car_type'
where_clause = ""
if filters:
    filter_conditions = []
    for col, val in filters.items():
        if isinstance(val, str):
            filter_conditions.append(f"{col} = '{val}'")
        elif isinstance(val, (int, float)):
            filter_conditions.append(f"{col} = {val}")
    if filter_conditions:
        where_clause = " WHERE " + " AND ".join(filter_conditions)

select_sql = f"SELECT {name} FROM {config['database']}.{table_name}{where_clause};"

print(select_sql)