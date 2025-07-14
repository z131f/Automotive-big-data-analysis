from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import os
import uuid
from collections import defaultdict
from func import read_data_with_filters, insert_data  # Ensure insert_data is imported
import json  # Import json module

app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 字段映射字典（数据库字段 -> 前端字段）
FIELD_MAPPING = {
    'car_brand': 'brand',
    'car_model': 'model',
    'manufacturer_suggested_price': 'guide_price',
    'engine_horsepower': 'horsepower',
    'num_doors': 'doors',
    'min_reference_price': 'min_price',
    'popularity': 'attention',
    'discount_percentage': 'discount',
    'car_type': 'car_type',
    'manufacture_year': 'manufacture_year'
}

# 反转映射（前端字段 -> 数据库字段）
REVERSE_MAPPING = {v: k for k, v in FIELD_MAPPING.items()}


# 获取数据库数据
def fetch_car_data():
    """从Hive获取所有车型数据并转换为前端格式"""
    raw_data = read_data_with_filters(name='*')['data']

    # 转换字段名和结构
    cars = []
    for item in raw_data:
        car = {}
        for db_field, front_field in FIELD_MAPPING.items():
            car[front_field] = item.get(db_field)

        # 添加原始数据中的关键字段（不在映射中）
        # Ensure these are parsed from string if they come from Hive as such
        city_license_plates_raw = item.get('city_license_plates', {})
        if isinstance(city_license_plates_raw, str):
            try:
                car['city_license_plates'] = json.loads(city_license_plates_raw)
            except json.JSONDecodeError:
                car['city_license_plates'] = {}  # Fallback if not valid JSON
        else:
            car['city_license_plates'] = city_license_plates_raw

        car['manufacture_year'] = item.get('manufacture_year')

        # 处理历史价格
        history_prices = []
        historical_price_raw = item.get('historical_price', {})
        if isinstance(historical_price_raw, str):
            try:
                historical_price_dict = json.loads(historical_price_raw)
            except json.JSONDecodeError:
                historical_price_dict = {}  # Fallback if not valid JSON
        else:
            historical_price_dict = historical_price_raw

        if isinstance(historical_price_dict, dict):
            for date, price in historical_price_dict.items():
                history_prices.append({'date': date, 'price': price})
        car['history_prices'] = history_prices

        # 生成唯一ID（使用品牌+车型）
        car['id'] = f"{car['brand']}_{car['model']}".replace(" ", "_")
        car['model_id'] = car['id']

        cars.append(car)
    return cars


def fetch_city_data():
    """从Hive获取城市上牌量数据"""
    raw_data = read_data_with_filters(name='city, city_license_plates')['data']

    # 汇总城市数据
    city_registrations = {}
    for item in raw_data:
        city_license_plates_raw = item.get('city_license_plates')

        # Parse if it's a string (e.g., from Hive)
        if isinstance(city_license_plates_raw, str):
            try:
                city_license_plates_dict = json.loads(city_license_plates_raw)
            except json.JSONDecodeError:
                city_license_plates_dict = {}  # Fallback
        else:
            city_license_plates_dict = city_license_plates_raw

        if not city_license_plates_dict:
            continue

        if isinstance(city_license_plates_dict, dict):
            for city, count in city_license_plates_dict.items():
                city_registrations[city] = city_registrations.get(city, 0) + count

    # 转换为前端格式
    cities = []
    for city_id, (city, registrations) in enumerate(city_registrations.items()):
        cities.append({
            'id': city_id,
            'city': city,
            'registrations': registrations
        })
    return cities


def fetch_market_trends_data():
    """从真实数据获取市场趋势数据"""
    cars = fetch_car_data()

    # 按年份分组
    year_data = defaultdict(lambda: {
        'registrations': 0,
        'attention': 0,
        'price_sum': 0,
        'count': 0
    })

    # 计算各年份的统计数据
    for car in cars:
        year = car.get('manufacture_year')
        if not year:
            continue

        # 计算该车型的总注册量（所有城市）
        registrations = 0
        license_plates_raw = car.get('city_license_plates', {})
        # Ensure city_license_plates is a dict before summing values
        if isinstance(license_plates_raw, str):  # This might happen if fetch_car_data didn't parse it
            try:
                license_plates = json.loads(license_plates_raw)
            except json.JSONDecodeError:
                license_plates = {}
        else:
            license_plates = license_plates_raw

        if isinstance(license_plates, dict):
            registrations = sum(license_plates.values())

        # 累加数据
        year_data[year]['registrations'] += registrations
        year_data[year]['attention'] += car.get('attention', 0)
        year_data[year]['price_sum'] += car.get('guide_price', 0)
        year_data[year]['count'] += 1

    # 构建趋势数据
    trends = []
    for year, data in sorted(year_data.items()):
        if data['count'] > 0:
            trends.append({
                'date': str(year),
                'registrations': data['registrations'],
                'attention': data['attention'],
                'avg_price': data['price_sum'] / data['count']
            })

    return trends


def fetch_consumer_preferences():
    """从真实数据获取消费者偏好数据"""
    cars = fetch_car_data()

    # 计算总注册量
    total_registrations = 0
    for car in cars:
        license_plates_raw = car.get('city_license_plates')
        # Ensure city_license_plates is a dict before summing values
        if isinstance(license_plates_raw, str):
            try:
                license_plates = json.loads(license_plates_raw)
            except json.JSONDecodeError:
                license_plates = {}
        else:
            license_plates = license_plates_raw

        if isinstance(license_plates, dict):
            total_registrations += sum(license_plates.values())

    if total_registrations == 0:
        return []

    # 按车型类型分组
    type_data = defaultdict(int)
    for car in cars:
        car_type = car.get('car_type', '')
        # 将"新能源"替换为"电动汽车"
        if car_type == '新能源':
            car_type = '电动汽车'

        license_plates_raw = car.get('city_license_plates')
        # Ensure city_license_plates is a dict before summing values
        if isinstance(license_plates_raw, str):
            try:
                license_plates = json.loads(license_plates_raw)
            except json.JSONDecodeError:
                license_plates = {}
        else:
            license_plates = license_plates_raw

        if isinstance(license_plates, dict):
            type_data[car_type] += sum(license_plates.values())

    # 构建偏好数据
    preferences = []
    for car_type, count in type_data.items():
        preferences.append({
            'type': car_type,
            'preference': count / total_registrations
        })

    return preferences


@app.route('/')
def index():
    return render_template('index.html')


# 数据上传API（保持不变）
# 在 app.py 的 upload_excel 函数中修改以下部分
@app.route('/api/v1/upload/excel', methods=['POST'])
def upload_excel():
    if 'excelFile' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['excelFile']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # 验证文件扩展名
    if not file.filename.lower().endswith(('.xls', '.xlsx')):
        return jsonify({'error': 'Invalid file format'}), 400

    try:
        # 生成唯一文件名防止冲突
        unique_filename = str(uuid.uuid4()) + os.path.splitext(file.filename)[1]
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)

        # 保存文件
        file.save(file_path)

        # 尝试解析Excel
        try:
            # 验证Excel文件有效性
            df = pd.read_excel(file_path)

            # 检查是否为空文件
            if df.empty:
                return jsonify({'error': 'Excel file is empty'}), 400

            # 1. 将DataFrame转换为字典列表
            data_list = df.to_dict(orient='records')

            # 2. 转换字段名：前端字段 -> 数据库字段
            for record in data_list:
                # Create a new dictionary to store converted fields
                converted_record = {}

                # Convert mapped fields
                for front_field, db_field in REVERSE_MAPPING.items():
                    if front_field in record:
                        converted_record[db_field] = record[front_field]

                # Add non-mapped fields (directly use database field names)
                non_mapped_fields = ['city', 'manufacture_year', 'fuel_capacity',
                                     'historical_price', 'city_license_plates']
                for field in non_mapped_fields:
                    if field in record:
                        value = record[field]
                        # IMPORTANT: Parse JSON strings back to Python objects for MAP types
                        if field in ['historical_price', 'city_license_plates'] and isinstance(value, str):
                            try:
                                converted_record[field] = json.loads(value)
                            except json.JSONDecodeError:
                                # Handle cases where the string might not be valid JSON
                                converted_record[field] = {}  # Default to empty dict or handle error
                        else:
                            converted_record[field] = value

                # Update the original record
                record.clear()
                record.update(converted_record)

            # 3. 插入数据到Hive
            insert_data(data_list)

            processed_count = len(df)

            # Actual application logic would involve database insertion here
            return jsonify({
                'status': 'success',
                'message': f'成功插入{processed_count} 行数到表'
            }), 200
        except Exception as e:
            # Catch Excel parsing errors or database insertion errors
            app.logger.error(f'Error processing Excel file or inserting data: {str(e)}')
            return jsonify({'error': f'Invalid Excel file content or database error: {str(e)}'}), 400
        finally:
            # Clean up the uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)
    except Exception as e:
        app.logger.error(f'File upload error: {str(e)}')
        return jsonify({'error': 'Internal server error'}), 500


# 品牌与车型深度分析API
@app.route('/api/v1/brands', methods=['GET'])
def get_brands():
    cars = fetch_car_data()
    brands = list(set(car['brand'] for car in cars))
    return jsonify({'brands': brands}), 200


@app.route('/api/v1/brands/<brand_name>/models', methods=['GET'])
def get_brand_models(brand_name):
    cars = fetch_car_data()
    models = [{'id': car['model_id'], 'name': car['model']}
              for car in cars if car['brand'] == brand_name]
    return jsonify({'models': models}), 200


@app.route('/api/v1/models/<model_id>', methods=['GET'])
def get_model_details(model_id):
    cars = fetch_car_data()
    car = next((car for car in cars if car['model_id'] == model_id), None)
    if not car:
        return jsonify({'error': 'Model not found'}), 404

    # 复制对象并删除不需要的字段
    details = car.copy()
    details.pop('id', None)
    return jsonify(details), 200


# 区域市场分析API
@app.route('/api/v1/cities', methods=['GET'])
def get_cities():
    cities = fetch_city_data()
    city_list = [{'id': city['id'], 'name': city['city']} for city in cities]
    return jsonify({'cities': city_list}), 200


@app.route('/api/v1/cities/rankings', methods=['GET'])
def get_city_rankings():
    cities = fetch_city_data()
    metric = request.args.get('metric', 'registrations')

    if metric not in ['registrations', 'attention']:
        return jsonify({'error': 'Invalid metric'}), 400

    sorted_cities = sorted(cities, key=lambda x: x.get(metric, 0), reverse=True)
    result = [{'city': city['city'], metric: city.get(metric, 0)} for city in sorted_cities]
    return jsonify({'rankings': result}), 200


# 消费者建议API
@app.route('/api/v1/recommendations', methods=['GET'])
def get_recommendations():
    cars = fetch_car_data()
    filters = {
        'brand': request.args.get('brand'),
        'min_price': request.args.get('min_price', type=float),
        'max_price': request.args.get('max_price', type=float),
        'min_hp': request.args.get('min_hp', type=int),
        'doors': request.args.get('doors', type=int),
        'car_type': request.args.get('car_type'),
    }

    filtered_cars = cars
    if filters['brand']:
        filtered_cars = [car for car in filtered_cars if car['brand'] == filters['brand']]

    if filters['min_price'] is not None:
        filtered_cars = [car for car in filtered_cars if car['min_price'] >= filters['min_price']]

    if filters['max_price'] is not None:
        filtered_cars = [car for car in filtered_cars if car['min_price'] <= filters['max_price']]

    if filters['min_hp'] is not None:
        filtered_cars = [car for car in filtered_cars if car['horsepower'] >= filters['min_hp']]

    if filters['doors'] is not None:
        filtered_cars = [car for car in filtered_cars if car['doors'] == filters['doors']]

    if filters['car_type']:
        filtered_cars = [car for car in filtered_cars if car['car_type'] == filters['car_type']]

    filtered_cars.sort(key=lambda x: x['attention'], reverse=True)
    recommendations = [{
        'id': car['model_id'],
        'brand': car['brand'],
        'model': car['model'],
        'min_price': car['min_price'],
        'horsepower': car['horsepower'],
        'car_type': car['car_type'],
        'attention': car['attention']
    } for car in filtered_cars]

    return jsonify({'recommendations': recommendations}), 200


# 市场分析API
@app.route('/api/v1/market/overview', methods=['GET'])
def market_overview():
    cities = fetch_city_data()
    cars = fetch_car_data()

    total_registrations = sum(city['registrations'] for city in cities)
    avg_attention = sum(car['attention'] for car in cars) / len(cars) if cars else 0

    brand_counts = defaultdict(int)
    for car in cars:
        brand_counts[car['brand']] += 1

    if cars:
        top_car = max(cars, key=lambda x: x['attention'])
        top_car_info = f"{top_car['brand']} {top_car['model']} (关注度: {top_car['attention']})"
    else:
        top_car_info = "无数据"

    return jsonify({
        'total_registrations': total_registrations,
        'avg_attention': avg_attention,
        'popular_brands': dict(brand_counts),
        'top_car': top_car_info
    }), 200


@app.route('/api/v1/market/trends', methods=['GET'])
def market_trends():
    metric = request.args.get('metric', 'registrations')
    if metric not in ['registrations', 'attention', 'avg_price']:
        return jsonify({'error': 'Invalid metric'}), 400

    # 获取真实市场趋势数据
    market_trends_data = fetch_market_trends_data()

    data_points = [{'date': point['date'], 'value': point[metric]} for point in market_trends_data]

    return jsonify({
        'metric': metric,
        'granularity': 'yearly',
        'data': data_points
    }), 200


@app.route('/api/v1/market/price_distribution', methods=['GET'])
def price_distribution():
    cars = fetch_car_data()
    # 定义价格区间（单位：元）
    price_ranges = [
        (0, 100_000),  # 0-10万元
        (100_000, 200_000),  # 10-20万元
        (200_000, 300_000),  # 20-30万元
        (300_000, 500_000),  # 30-50万元
        (500_000, float('inf'))  # 50万元以上
    ]

    distribution = []
    for min_price, max_price in price_ranges:
        # 转换价格区间为万元显示
        min_wan = min_price // 10_000
        if max_price == float('inf'):
            range_str = f"{min_wan}万以上"
            cars_in_range = [car for car in cars if car['min_price'] >= min_price]
        else:
            max_wan = max_price // 10_000
            range_str = f"{min_wan}万-{max_wan}万"
            cars_in_range = [car for car in cars if min_price <= car['min_price'] < max_price]

        count = len(cars_in_range)
        # 计算平均关注度（避免除以零）
        if count > 0:
            avg_attention = sum(car['attention'] for car in cars_in_range) / count
        else:
            avg_attention = 0

        distribution.append({
            'range': range_str,
            'count': count,
            'avg_attention': avg_attention
        })

    return jsonify({'distribution': distribution}), 200


# 消费者洞察API
@app.route('/api/v1/consumer_insights/preferences', methods=['GET'])
def consumer_preferences():
    dimension = request.args.get('dimension', 'type')

    if dimension == 'type':
        # 获取真实消费者偏好数据
        preferences = fetch_consumer_preferences()
        return jsonify(preferences), 200
    else:
        return jsonify([{
            'range': '100-150马力',
            'preference': 0.4
        }, {
            'range': '150-200马力',
            'preference': 0.35
        }, {
            'range': '200+马力',
            'preference': 0.25
        }]), 200


if __name__ == '__main__':
    app.run(debug=True, port=5000)
