from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# === 1. Hàm ETL từ bảng kpi_5min ===
def etl_from_kpi5min(**kwargs):
    # Kết nối tới PostgreSQL
    engine = create_engine("postgresql+psycopg2://restaurant:restaurant_pass@postgres:5432/restaurant_db")

    # Đọc dữ liệu KPI 5 phút gần nhất
    query = """
        SELECT * 
        FROM kpi_5min
        WHERE window_start >= NOW() - INTERVAL '1 day';
    """
    df = pd.read_sql(query, engine)
    print(f"📥 Đã đọc {len(df)} dòng dữ liệu từ kpi_5min")

    # Nếu không có dữ liệu, dừng sớm
    if df.empty:
        print("⚠️ Không có dữ liệu trong 24h qua. Dừng ETL.")
        return

    # Tính KPI trung bình theo branch
    df_daily = (
        df.groupby("branch_id")
        .agg({
            "avg_revenue": "mean",
            "avg_service_time": "mean",
            "avg_rating": "mean"
        })
        .reset_index()
    )

    # Thêm timestamp xử lý
    df_daily["processed_at"] = datetime.now()

    # Ghi kết quả ra bảng mới
    df_daily.to_sql("daily_kpi", engine, if_exists="replace", index=False)
    print("✅ Đã lưu dữ liệu tổng hợp vào bảng daily_kpi")

# === 2. Cấu hình DAG ===
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='restaurant_etl_realdata',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL from real KPI data (restaurant_db.kpi_5min)'
) as dag:

    etl_task = PythonOperator(
        task_id='etl_from_kpi5min',
        python_callable=etl_from_kpi5min,
        provide_context=True
    )

    etl_task
