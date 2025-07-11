from impala.dbapi import connect
import logging

# 假設 HIVE_CONFIG 已經定義，例如：
# HIVE_CONFIG = {
#     'host': 'your_hive_host',
#     'port': 21050,
#     'database': 'your_database_name',
#     'auth_mechanism': 'PLAIN',
# }

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_hive_table(table_name, schema, config):
    """
    在 Hive 中創建數據表，适配 car_data 表結構。

    Args:
        table_name (str): 要創建的表名 (例如 'car_data')。
        schema (dict): 表的 schema 定義，鍵為列名，值為 Hive 數據類型字符串。
        config (dict): Hive 連接配置。

    Returns:
        dict: 包含操作結果的字典。
    """
    conn = None
    try:
        conn = connect(**config)
        cursor = conn.cursor()

        drop_sql = 'DROP TABLE IF EXISTS car_data'

        cursor.execute(drop_sql)

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
        logging.info(f"執行建表 SQL:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        return {"status": "success", "message": f"表 '{table_name}' 創建成功或已存在。"}

    except Exception as e:
        logging.error(f"創建表 '{table_name}' 失敗: {e}")
        return {"status": "error", "message": f"創建表失敗: {e}"}
    finally:
        if conn:
            conn.close()


def insert_into_hive_table(table_name, data, schema, config):
    """
    將數據插入到 Hive 表中，适配 car_data 表結構，並處理 ARRAY 和 MAP 類型。

    Args:
        table_name (str): 目標表名 (例如 'car_data')。
        data (list[dict]): 要插入的數據列表，每個字典代表一行。
        schema (dict): 表的 schema 定義，用於判斷數據類型以便正確格式化。
        config (dict): Hive 連接配置。

    Returns:
        dict: 包含操作結果的字典。
    """
    if not data:
        return {"status": "warning", "message": "沒有提供數據，跳過插入。"}

    conn = None
    try:
        conn = connect(**config)
        cursor = conn.cursor()

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

        insert_sql = f"INSERT INTO TABLE {config['database']}.{table_name} VALUES {', '.join(all_rows_values)}"

        logging.info(f"執行插入 SQL (前500字符):\n{insert_sql[:500]}...")
        cursor.execute(insert_sql)
        return {"status": "success", "message": f"成功插入 {len(data)} 行數據到表 '{table_name}'。"}

    except Exception as e:
        logging.error(f"插入數據到表 '{table_name}' 失敗: {e}")
        return {"status": "error", "message": f"插入數據失敗: {e}"}
    finally:
        if conn:
            conn.close()


def read_from_hive_table(table_name, config, filters = None, name = '*'):
    """
    從 Hive 表中讀取數據。

    Args:
        table_name (str): 要讀取的表名。
        filters (dict, optional): 篩選條件。
        config (dict): Hive 連接配置。

    Returns:
        dict: 包含操作結果的字典。
    """
    conn = None
    try:
        conn = connect(**config)
        cursor = conn.cursor()

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

        select_sql = f"SELECT {name} FROM {config['database']}.{table_name}{where_clause}"
        logging.info(f"执行查询 SQL:\n{select_sql}")
        cursor.execute(select_sql)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))

        return {"status": "success", "data": results, "message": f"成功从表 '{table_name}' 读取 {len(results)} 行数据"}

    except Exception as e:
        logging.error(f"从表 '{table_name}' 读取数据失败: {e}")
        return {"status": "error", "message": f"读取数据失败: {e}"}
    finally:
        if conn:
            conn.close()