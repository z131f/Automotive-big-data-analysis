# test_app.py
import sys
import os
import pytest
from unittest.mock import patch, MagicMock
import json
import pandas as pd

# 添加当前目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 现在可以导入 app
from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


# Mock data for testing (严格遵循 car_data_schema 格式)
MOCK_CAR_DATA = [
    {
        'car_brand': 'Brand1',
        'car_model': 'Model1',
        'manufacturer_suggested_price': 85000.00,
        'engine_horsepower': 150,
        'num_doors': 4,
        'min_reference_price': 80000.00,
        'car_type': 'Sedan',
        'manufacture_year': 2020,
        'popularity': 75,
        'discount_percentage': 5.0,
        'historical_price': {'2023-01': 90000, '2023-02': 88000},
        'city_license_plates': {'CityA': 50, 'CityB': 25}
    },
    {
        'car_brand': 'Brand1',
        'car_model': 'Model2',
        'manufacturer_suggested_price': 250000.00,
        'engine_horsepower': 250,
        'num_doors': 5,
        'min_reference_price': 220000.00,
        'car_type': 'SUV',
        'manufacture_year': 2021,
        'popularity': 90,
        'discount_percentage': 3.5,
        'historical_price': {'2023-01': 240000, '2023-02': 235000},
        'city_license_plates': {'CityA': 30, 'CityC': 40}
    },
    {
        'car_brand': 'Brand2',
        'car_model': 'Model1',
        'manufacturer_suggested_price': 380000.00,
        'engine_horsepower': 300,
        'num_doors': 2,
        'min_reference_price': 350000.00,
        'car_type': 'Sports',
        'manufacture_year': 2022,
        'popularity': 85,
        'discount_percentage': 2.0,
        'historical_price': {'2023-01': 370000, '2023-02': 365000},
        'city_license_plates': {'CityB': 60, 'CityC': 20}
    },
    {
        'car_brand': 'Brand3',
        'car_model': 'Model1',
        'manufacturer_suggested_price': 600000.00,
        'engine_horsepower': 400,
        'num_doors': 2,
        'min_reference_price': 550000.00,
        'car_type': 'Luxury',
        'manufacture_year': 2023,
        'popularity': 95,
        'discount_percentage': 1.5,
        'historical_price': {'2023-01': 590000, '2023-02': 585000},
        'city_license_plates': {'CityA': 10, 'CityD': 30}
    }
]

MOCK_CITY_DATA = [
    {'city': 'CityA', 'city_license_plates': {'CityA': 90}},
    {'city': 'CityB', 'city_license_plates': {'CityB': 85}},
    {'city': 'CityC', 'city_license_plates': {'CityC': 60}},
    {'city': 'CityD', 'city_license_plates': {'CityD': 30}}
]


def mock_read_data_with_filters(**kwargs):
    # 模拟 read_data_with_filters 函数的响应
    if 'name' in kwargs and kwargs['name'] == '*':
        return {'status': 'success', 'data': MOCK_CAR_DATA}
    elif 'name' in kwargs and kwargs['name'] == 'city, city_license_plates':
        return {'status': 'success', 'data': MOCK_CITY_DATA}
    return {'status': 'success', 'data': []}


@pytest.fixture(autouse=True)
def mock_dependencies():
    # 模拟 func.py 中的 read_data_with_filters 函数
    with patch('app.read_data_with_filters', new=mock_read_data_with_filters):
        yield


# 测试用例
def test_index(client):
    """测试首页路由"""
    response = client.get('/')
    assert response.status_code == 200
    assert 'text/html' in response.content_type


def test_get_brands(client):
    """测试获取品牌列表"""
    response = client.get('/api/v1/brands')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'brands' in data
    assert sorted(data['brands']) == ['Brand1', 'Brand2', 'Brand3']


def test_get_brand_models(client):
    """测试获取品牌下的车型列表"""
    response = client.get('/api/v1/brands/Brand1/models')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'models' in data
    assert len(data['models']) == 2
    assert {'id': 'Brand1_Model1', 'name': 'Model1'} in data['models']
    assert {'id': 'Brand1_Model2', 'name': 'Model2'} in data['models']


def test_get_model_details(client):
    """测试获取车型详情"""
    response = client.get('/api/v1/models/Brand1_Model1')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['brand'] == 'Brand1'
    assert data['model'] == 'Model1'
    assert data['guide_price'] == 85000.00
    assert data['horsepower'] == 150
    assert len(data['history_prices']) == 2


def test_get_cities(client):
    """测试获取城市列表"""
    response = client.get('/api/v1/cities')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'cities' in data
    assert len(data['cities']) == 4
    city_names = [city['name'] for city in data['cities']]
    assert sorted(city_names) == ['CityA', 'CityB', 'CityC', 'CityD']


def test_get_city_rankings(client):
    """测试获取城市排名"""
    response = client.get('/api/v1/cities/rankings?metric=registrations')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'rankings' in data
    # 验证按注册量排序
    registrations = [city['registrations'] for city in data['rankings']]
    assert registrations == sorted(registrations, reverse=True)
    # 验证城市注册量正确
    city_registrations = {city['city']: city['registrations'] for city in data['rankings']}
    assert city_registrations['CityA'] == 90
    assert city_registrations['CityB'] == 85
    assert city_registrations['CityC'] == 60
    assert city_registrations['CityD'] == 30


def test_get_recommendations(client):
    """测试获取消费者推荐"""
    response = client.get('/api/v1/recommendations?brand=Brand1&min_price=80000&max_price=250000')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'recommendations' in data
    assert len(data['recommendations']) == 2
    models = [rec['model'] for rec in data['recommendations']]
    assert 'Model1' in models
    assert 'Model2' in models


def test_market_overview(client):
    """测试市场概览"""
    response = client.get('/api/v1/market/overview')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'total_registrations' in data
    assert data['total_registrations'] == 90 + 85 + 60 + 30  # 所有城市注册量总和
    assert 'avg_attention' in data
    assert data['popular_brands'] == {'Brand1': 2, 'Brand2': 1, 'Brand3': 1}


def test_market_trends(client):
    """测试市场趋势"""
    response = client.get('/api/v1/market/trends?metric=registrations')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'metric' in data
    assert 'data' in data
    # 验证年份数据
    years = [point['date'] for point in data['data']]
    assert sorted(years) == ['2020', '2021', '2022', '2023']


def test_price_distribution(client):
    """测试价格分布（重点测试）"""
    response = client.get('/api/v1/market/price_distribution')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'distribution' in data
    distribution = data['distribution']

    # 验证价格区间
    ranges = [item['range'] for item in distribution]
    assert ranges == [
        '0万-10万',
        '10万-20万',
        '20万-30万',
        '30万-50万',
        '50万以上'
    ]

    # 验证车辆数量分布
    counts = [item['count'] for item in distribution]
    assert counts == [1, 0, 1, 1, 1]  # 根据MOCK_CAR_DATA中的数据

    # 验证平均关注度计算
    assert distribution[0]['avg_attention'] == 75.0  # 0-10万区间只有Brand1 Model1
    assert distribution[2]['avg_attention'] == 90.0  # 20-30万区间只有Brand1 Model2
    assert distribution[3]['avg_attention'] == 85.0  # 30-50万区间只有Brand2 Model1
    assert distribution[4]['avg_attention'] == 95.0  # 50万以上区间只有Brand3 Model1


def test_consumer_preferences(client):
    """测试消费者偏好"""
    response = client.get('/api/v1/consumer_insights/preferences?dimension=type')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 4
    # 验证偏好总和接近1.0
    total_preference = sum(item['preference'] for item in data)
    assert abs(total_preference - 1.0) < 0.0001


def test_upload_excel_success(client, tmp_path):
    """测试Excel上传成功"""
    # 使用 .xlsx 格式代替 .xls
    test_file = tmp_path / "test.xlsx"

    # 使用 openpyxl 引擎（无需指定，pandas 会自动选择）
    df = pd.DataFrame({
        'car_brand': ['Toyota', 'Honda'],
        'car_model': ['Camry', 'Accord'],
        'manufacturer_suggested_price': [250000, 220000]
    })
    df.to_excel(test_file, index=False)  # 移除 engine='xlwt'

    # 模拟上传（使用 .xlsx 扩展名）
    with open(test_file, 'rb') as f:
        response = client.post(
            '/api/v1/upload/excel',
            data={'excelFile': (f, 'test.xlsx')},  # 改为 .xlsx
            content_type='multipart/form-data'
        )

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['status'] == 'success'
    assert data['message'] == 'Processed 2 rows'


def test_upload_excel_no_file(client):
    """测试无文件上传"""
    response = client.post('/api/v1/upload/excel')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data
    assert 'No file part' in data['error']


def test_upload_excel_invalid_format(client, tmp_path):
    """测试无效文件格式上传"""
    # 创建非Excel文件
    test_file = tmp_path / "test.txt"
    test_file.write_text("dummy content")

    # 模拟上传
    with open(test_file, 'rb') as f:
        response = client.post(
            '/api/v1/upload/excel',
            data={'excelFile': (f, 'test.txt')},
            content_type='multipart/form-data'
        )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data
    assert 'Invalid file format' in data['error']


def test_upload_excel_invalid_content(client, tmp_path):
    """测试无效Excel内容"""
    # 创建无效的Excel文件（文本内容）
    test_file = tmp_path / "invalid.xlsx"
    test_file.write_text("This is not a valid Excel file")

    # 模拟上传
    with open(test_file, 'rb') as f:
        response = client.post(
            '/api/v1/upload/excel',
            data={'excelFile': (f, 'invalid.xlsx')},
            content_type='multipart/form-data'
        )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data
    assert 'Invalid Excel file content' in data['error']


def test_upload_excel_empty_file(client, tmp_path):
    """测试空Excel文件上传"""
    # 使用 .xlsx 格式
    test_file = tmp_path / "empty.xlsx"

    # 使用默认引擎
    df = pd.DataFrame()
    df.to_excel(test_file, index=False)  # 移除 engine='xlwt'

    # 模拟上传（使用 .xlsx 扩展名）
    with open(test_file, 'rb') as f:
        response = client.post(
            '/api/v1/upload/excel',
            data={'excelFile': (f, 'empty.xlsx')},  # 改为 .xlsx
            content_type='multipart/form-data'
        )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'error' in data
    assert 'Excel file is empty' in data['error']