import os, psutil, subprocess, time, platform

def get_system_info():
    return{
        "CPU Cores": psutil.cpu_count(logical=True),
        "CPU Frequency (MHz)": psutil.cpu_freq().max,
        "Total Memory (GB)": round(psutil.virtual_memory().total / (1024 ** 3), 2),
        "Processor": platform.processor(),
        "System": platform.system(),
        "Version": platform.version(),
    }
    
print("🖥️ System Information:")
for key, value in get_system_info().items():
    print(f"  {key}: {value}")
    
start_time = time.time()
start_cpu = psutil.cpu_percent(interval=1)

try:
    print("Spark Text Processing")
    spark_process = subprocess.run(["spark-submit", "text_processor.py"], check=True)
    
    print("Starting Airflow DAG")
    airflow_process = subprocess.run(["airflow", "dags", "trigger", "text_processing_pipeline"], check=True)
    
    end_time = time.time()
    end_cpu = psutil.cpu_percent(interval=1)
    
    print("Execution Time", round(end_time - start_time, 2), "seconds")
    print("CPU Usage during Execution", round((start_cpu + end_cpu) / 2, 2), "%")
    
    print("Pipeline finished. (Find the result in output/ folder)")
    
except Exception as e:
    print("Error Occurred: ". str(e))