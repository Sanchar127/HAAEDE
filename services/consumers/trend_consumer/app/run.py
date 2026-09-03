from prometheus_client import start_http_server

from services.consumers.trend_consumer.app.worker import start_worker

if __name__ == "__main__":
   
    start_http_server(8002)

    start_worker()