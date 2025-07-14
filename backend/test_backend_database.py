import pytest
import json
import os
import sys
import time
import pandas as pd
from unittest.mock import patch, MagicMock

# Add the parent directory to the sys.path to allow imports from app, func, utils, config
# This assumes test_backend_db.py is in the same directory as app.py, func.py, etc.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import REVERSE_MAPPING directly from app
from app import app, fetch_car_data, fetch_city_data, fetch_market_trends_data, fetch_consumer_preferences, \
    REVERSE_MAPPING
from func import setup_environment, insert_data, read_data_with_filters, rand_data_generate
from utils import create_hive_table, insert_into_hive_table, read_from_hive_table
from config import HIVE_CONFIG, car_data_schema

# Number of records to generate for testing
NUM_TEST_RECORDS = 20


@pytest.fixture(scope="module")
def client():
    """
    Pytest fixture to set up the Flask test client and prepare the Hive database.
    This runs once per module.
    """
    print("\n--- Setting up test environment (Hive table & data) ---")

    # 1. Initialize Hive table (drops if exists, then creates)
    # This directly calls the utility function to ensure table setup
    create_table_result = create_hive_table(
        table_name='car_data',
        schema=car_data_schema,
        config=HIVE_CONFIG
    )
    print(f"Hive table setup result: {create_table_result}")
    assert create_table_result['status'] == 'success'

    # 2. Generate random test data
    test_data = rand_data_generate(NUM_TEST_RECORDS)
    print(f"Generated {len(test_data)} random test records.")

    # 3. Insert test data into Hive
    insert_result = insert_into_hive_table(
        table_name='car_data',
        data=test_data,
        schema=car_data_schema,
        config=HIVE_CONFIG
    )
    print(f"Hive data insertion result: {insert_result}")
    assert insert_result['status'] == 'success'
    assert "成功插入" in insert_result['message']  # Check for success message

    # Give Hive/Impala a moment to process (especially if running quickly)
    time.sleep(2)

    # Use Flask's test_client
    with app.test_client() as client:
        yield client  # This is where the tests run

    print("\n--- Tearing down test environment (dropping Hive table) ---")
    # Clean up: Drop the table after all tests are done
    conn = None
    try:
        from impala.dbapi import connect
        conn = connect(**HIVE_CONFIG)
        cursor = conn.cursor()
        drop_sql = f"DROP TABLE IF EXISTS {HIVE_CONFIG['database']}.car_data"
        cursor.execute(drop_sql)
        print(f"Successfully dropped table 'car_data'.")
    except Exception as e:
        print(f"Error during Hive table teardown: {e}")
    finally:
        if conn:
            conn.close()


# --- Helper function to create a fake Excel file ---
def create_fake_excel_file(file_path, data_rows):
    """
    Creates a fake Excel file with specified data.
    data_rows: List of dictionaries, where each dict is a row.
               Keys should match the frontend field names.
    """
    # Map backend field names to frontend field names for the Excel columns
    # This is crucial because the upload expects frontend names in the Excel.
    # We need to reverse the REVERSE_MAPPING from app.py to get frontend -> backend
    # Changed from app.REVERSE_MAPPING to REVERSE_MAPPING
    FIELD_MAPPING_REVERSED = {v: k for k, v in REVERSE_MAPPING.items()}

    # Prepare data for DataFrame, ensuring correct column names (frontend names)
    excel_data = []
    for row in data_rows:
        excel_row = {}
        # Changed from app.REVERSE_MAPPING to REVERSE_MAPPING
        for front_field, db_field in REVERSE_MAPPING.items():
            if db_field in row:
                excel_row[front_field] = row[db_field]

        # Add non-mapped fields directly using their database names (which are also frontend names in this case)
        non_mapped_fields = ['city', 'manufacture_year', 'fuel_capacity',
                             'historical_price', 'city_license_plates']
        for field in non_mapped_fields:
            if field in row:
                # For MAP types, ensure they are stringified if they are dicts
                if isinstance(row[field], dict):
                    excel_row[field] = json.dumps(row[field])
                else:
                    excel_row[field] = row[field]
        excel_data.append(excel_row)

    df = pd.DataFrame(excel_data)
    df.to_excel(file_path, index=False)
    print(f"Created fake Excel file: {file_path}")


def test_upload_excel_and_verify_data(client):
    """
    Test uploading an Excel file and verifying the data is inserted and retrievable.
    """
    temp_excel_file = "test_upload_data.xlsx"

    # Sample data to upload (using database field names, will be mapped to frontend for Excel)
    upload_data = [
        {
            'car_brand': '测试品牌',
            'car_model': '测试车型X',
            'manufacturer_suggested_price': 150000.00,
            'engine_horsepower': 180,
            'num_doors': 4,
            'min_reference_price': 140000.00,
            'car_type': '轿车',
            'manufacture_year': 2023,
            'fuel_capacity': 55.0,
            'popularity': 950,
            'discount_percentage': 5.0,
            'historical_price': {'2023-01': 145000, '2022-07': 155000},
            'city_license_plates': {'北京': 500, '上海': 300}
        },
        {
            'car_brand': '测试品牌',
            'car_model': '测试车型Y',
            'manufacturer_suggested_price': 250000.00,
            'engine_horsepower': 220,
            'num_doors': 5,
            'min_reference_price': 230000.00,
            'car_type': 'SUV',
            'manufacture_year': 2024,
            'fuel_capacity': 60.0,
            'popularity': 880,
            'discount_percentage': 8.0,
            'historical_price': {'2024-03': 240000, '2023-09': 260000},
            'city_license_plates': {'广州': 400, '深圳': 600}
        }
    ]

    # Create the fake Excel file
    create_fake_excel_file(temp_excel_file, upload_data)

    try:
        with open(temp_excel_file, 'rb') as f:
            response = client.post(
                '/api/v1/upload/excel',
                data={'excelFile': (f, temp_excel_file)},
                content_type='multipart/form-data'
            )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'success'
        assert f"成功插入{len(upload_data)} 行数到表" in data['message']

        # Give Hive/Impala a moment to process the new insertion
        time.sleep(3)

        # Verify data presence via API endpoints
        # Check if the new brand '测试品牌' exists
        brands_response = client.get('/api/v1/brands')
        brands_data = json.loads(brands_response.data)
        assert '测试品牌' in brands_data['brands']

        # Check if models under '测试品牌' are present
        models_response = client.get('/api/v1/brands/测试品牌/models')
        models_data = json.loads(models_response.data)
        uploaded_model_names = [car['car_model'] for car in upload_data]
        found_models = [m['name'] for m in models_data['models'] if m['name'] in uploaded_model_names]
        assert len(found_models) == len(uploaded_model_names)

        # Verify details of one of the uploaded models
        # Construct model_id as per app.py logic
        test_model_id = f"{upload_data[0]['car_brand']}_{upload_data[0]['car_model']}".replace(" ", "_")
        details_response = client.get(f'/api/v1/models/{test_model_id}')
        details_data = json.loads(details_response.data)

        assert details_response.status_code == 200
        assert details_data['brand'] == upload_data[0]['car_brand']
        assert details_data['model'] == upload_data[0]['car_model']
        assert details_data['min_price'] == upload_data[0]['min_reference_price']
        assert details_data['horsepower'] == upload_data[0]['engine_horsepower']
        assert details_data['car_type'] == upload_data[0]['car_type']
        assert details_data['manufacture_year'] == upload_data[0]['manufacture_year']

        # Verify historical_price and city_license_plates are correctly fetched and formatted
        # Note: fetch_car_data converts 'historical_price' dict to a list of {'date': date, 'price': price}
        # and 'city_license_plates' remains a dict.
        assert isinstance(details_data['history_prices'], list)
        assert len(details_data['history_prices']) > 0
        assert isinstance(details_data['city_license_plates'], dict)
        assert len(details_data['city_license_plates']) > 0

    finally:
        # Clean up the fake Excel file
        if os.path.exists(temp_excel_file):
            os.remove(temp_excel_file)
            print(f"Cleaned up fake Excel file: {temp_excel_file}")


