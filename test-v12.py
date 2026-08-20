from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from google.protobuf.json_format import MessageToDict
import uvicorn
import gzip
import math
import time
import threading

app = FastAPI(title="JANUS Quant Engine - Ultra-Fast Flat Ingester")

# --- HỆ THỐNG LƯU TRỮ TRÊN RAM (THREAD-SAFE STATE) ---
WELFORD_STORE = {}
STATE_LOCK = threading.Lock()
WINDOW_SIZE = 30
TARGET_NAMESPACE = "bank-of-anthos"

# 🚀 BỘ ĐỆM ĐỂ GOM IN METRICS MỖI 3 GIÂY (CONCURRENCY-SAFE)
DISPLAY_BUFFER = {}
BUFFER_LOCK = threading.Lock()

# Map ánh xạ tối ưu O(1) tên metric hạ tầng
METRIC_MAP = {
    "container_cpu_usage_seconds_total": "cpu",
    "container_memory_working_set_bytes": "ram"
}

def process_welford_stateful(source_id, metric_name, val):
    """Thuật toán Welford trượt ổn định số học để tính Z-Score"""
    welford_key = f"{source_id}:{metric_name}"
    if welford_key not in WELFORD_STORE:
        WELFORD_STORE[welford_key] = {"samples": [], "mean": 0.0, "M2": 0.0}
    
    w_state = WELFORD_STORE[welford_key]
    
    if len(w_state["samples"]) >= WINDOW_SIZE:
        old_val = w_state["samples"].pop(0)
        old_mean = w_state["mean"]
        n = len(w_state["samples"])
        if n > 0:
            w_state["mean"] = (old_mean * (n + 1) - old_val) / n
            w_state["M2"] -= (old_val - old_mean) * (old_val - w_state["mean"])
            if w_state["M2"] < 0: w_state["M2"] = 0.0
            
    w_state["samples"].append(val)
    n = len(w_state["samples"])
    old_mean = w_state["mean"]
    w_state["mean"] += (val - old_mean) / n
    w_state["M2"] += (val - old_mean) * (val - w_state["mean"])
    
    z_score = 0.0
    if n > 1:
        variance = w_state["M2"] / (n - 1)
        std_dev = math.sqrt(variance)
        if std_dev > 1e-5:
            z_score = (val - w_state["mean"]) / std_dev
            
    return z_score

# 🎯 BACKGROUND WORKER THREAD: Tự động thức dậy mỗi 3s sắp xếp theo thời gian và xuất bảng
def periodic_display_worker():
    while True:
        time.sleep(3.0) # Chu kỳ giữ nhịp đúng 3 giây
        with BUFFER_LOCK:
            if not DISPLAY_BUFFER:
                continue
            print(f"\n╔═════════════════════════ [📊 JANUS METRICS STREAM VIEW - UPDATED AT {time.strftime('%H:%M:%S')}] ═════════════════════════╗")
            # Sắp xếp hiển thị tuần tự theo mốc thời gian xuất hiện của metric và tên Pod
            sorted_buffer = sorted(DISPLAY_BUFFER.items(), key=lambda x: (x[1]['time'], x[1]['pod']))
            for _, info in sorted_buffer:
                print(f"║ [{info['time']}] Pod: {info['pod']:<38} | CPU: {info['cpu']:<10.3f} | RAM: {info['ram']:<12.0f} | Latency: {info['latency']:6.2f} µs ║")
            print("╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝")
            DISPLAY_BUFFER.clear() # Dọn dẹp sạch bộ đệm để hứng chu kỳ 3s tiếp theo

# Kích hoạt luồng chạy ngầm ngay khi khởi chạy Engine
threading.Thread(target=periodic_display_worker, daemon=True).start()

# --- TẦNG TIẾP NHẬN MẠNG HTTP ---
@app.get("/")
@app.get("/v1/metrics")
async def health_check(): return {"status": "healthy"}

@app.options("/v1/metrics")
async def options_check(): return JSONResponse(content="OK", headers={"Allow": "POST, GET, OPTIONS"})

@app.post("/v1/metrics", status_code=status.HTTP_200_OK)
async def validate_metrics(request: Request):
    # ⏱️ TỐI ƯU TOÀN DIỆN: Bắt đầu đo thời gian ngay khi gói tin vừa chạm vào API của Engine
    api_start_time_ns = time.perf_counter_ns()
    
    try:
        raw_body = await request.body()
        if raw_body.startswith(b'\x1f\x8b'):
            raw_body = gzip.decompress(raw_body)
            
        export_request = metrics_service_pb2.ExportMetricsServiceRequest()
        export_request.ParseFromString(raw_body)
        
        payload = MessageToDict(export_request, use_integers_for_enums=True)
        
        for rm in payload.get("resourceMetrics", []):
            node_name = "unknown-node"
            for attr in rm.get("resource", {}).get("attributes", []):
                if attr.get("key") == "net.host.name":
                    node_name = attr.get("value", {}).get("stringValue", "unknown-node")
                    break
                    
            for sm in rm.get("scopeMetrics", []):
                for metric in sm.get("metrics", []):
                    m_name = metric.get("name", "")
                    
                    if m_name not in METRIC_MAP:
                        continue
                        
                    short_metric = METRIC_MAP[m_name]
                    data_points = metric.get("gauge", {}).get("dataPoints") or metric.get("sum", {}).get("dataPoints", [])
                    
                    for dp in data_points:
                        dp_attrs = dp.get("attributes", [])
                        pod_name = ""
                        namespace_name = ""
                        container_name = ""
                        
                        for attr in dp_attrs:
                            key = attr.get("key", "")
                            if key == "pod" or key == "k8s.pod.name":
                                pod_name = attr.get("value", {}).get("stringValue", "")
                            elif key == "namespace" or key == "k8s.namespace.name":
                                namespace_name = attr.get("value", {}).get("stringValue", "")
                            elif key == "container" or key == "k8s.container.name":
                                container_name = attr.get("value", {}).get("stringValue", "")
                                
                            if pod_name and namespace_name and container_name:
                                break
                                
                        if namespace_name == TARGET_NAMESPACE and pod_name and container_name and container_name != "POD":
                            val = dp.get("asDouble") or dp.get("asInt")
                            if val is not None:
                                final_source = f"{node_name}:{pod_name}:{container_name}"
                                
                                with STATE_LOCK:
                                    # Thực hiện Welford để giữ trạng thái ổn định cho dòng metric
                                    _ = process_welford_stateful(final_source, m_name, float(val))
                                
                                # Kết thúc đo thời gian xử lý toàn diện cho điểm metric này
                                duration_us = (time.perf_counter_ns() - api_start_time_ns) / 1000.0
                                
                                # Trích xuất mốc thời gian gốc của Metric từ Alloy đẩy sang
                                time_unix_sec = int(dp.get("timeUnixNano", 0)) // 1000000000
                                time_str = time.strftime('%H:%M:%S', time.localtime(time_unix_sec))
                                
                                # Tạo Key độc nhất kết hợp giữa Tên Pod và mốc Thời gian để chống trùng lặp dòng
                                display_key = f"{pod_name}@{time_str}"
                                
                                # 🚀 NẠP VÀO BỘ ĐỆM ĐỂ GOM IN MÀN HÌNH THEO CHU KỲ
                                with BUFFER_LOCK:
                                    if display_key not in DISPLAY_BUFFER:
                                        DISPLAY_BUFFER[display_key] = {
                                            "pod": pod_name,
                                            "time": time_str,
                                            "cpu": 0.0,
                                            "ram": 0.0,
                                            "latency": duration_us
                                        }
                                    
                                    DISPLAY_BUFFER[display_key][short_metric] = float(val)
                                    DISPLAY_BUFFER[display_key]["latency"] = duration_us
                                        
    except Exception:
        pass
        
    return {"status": "accepted"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5001, log_level="warning")

