import os
import uuid
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway


class PrometheusMetrics():
    def __init__(self):
        try:
            # Create a new registry
            self.registry = CollectorRegistry()

            # Define the required metrics
            # 1. Count of requests - Gauge
            self.request_counter = Gauge(
                'nersc_transfer_request_count',
                'Number of times the flow has been executed',
                ['execution_id'],
                registry=self.registry
            )

            self.bytes_counter = Gauge(
                'nersc_transfer_total_bytes',
                'Number of bytes for all the executed flows',
                ['execution_id'],
                registry=self.registry
            )

            # 2. Total bytes transferred - Gauge
            self.transfer_bytes = Gauge(
                'nersc_transfer_file_bytes',
                'Total size of all file transfers to NERSC',
                ['machine'],
                registry=self.registry
            )

            # 3. Transfer speed - Gauge
            self.transfer_speed = Gauge(
                'nersc_transfer_speed_bytes_per_second',
                'Transfer speed for NERSC file transfers in bytes per second',
                ['machine'],
                registry=self.registry
            )

            # 4. Transfer time - Gauge
            self.transfer_time = Gauge(
                'nersc_transfer_time_seconds',
                'Time taken for NERSC file transfers in seconds',
                ['machine'],
                registry=self.registry
            )
        except Exception as e:
            print(f"Error initializing Prometheus metrics: {e}")

    def push_metrics_to_prometheus(self, metrics, logger):
        """Push metrics directly to Prometheus Pushgateway."""
        PUSHGATEWAY_URL = os.getenv('PUSHGATEWAY_URL', 'http://localhost:9091')
        JOB_NAME = os.getenv('JOB_NAME', 'nersc_transfer')
        INSTANCE_LABEL = os.getenv('INSTANCE_LABEL', 'data_transfer')

        try:
            # Generate a unique execution ID for this transfer
            execution_id = f"exec_{str(uuid.uuid4())}"

            # Set the metrics
            self.request_counter.labels(execution_id=execution_id).set(1)
            self.bytes_counter.labels(execution_id=execution_id).set(metrics['bytes_transferred'])
            self.transfer_bytes.labels(machine=metrics['machine']).set(metrics['bytes_transferred'])
            self.transfer_time.labels(machine=metrics['machine']).set(metrics['duration_seconds'])
            self.transfer_speed.labels(machine=metrics['machine']).set(metrics['transfer_speed'])

            # Log metrics for debugging
            logger.info(f"Pushing metrics: transfer_bytes = {metrics['bytes_transferred']} bytes")
            logger.info(f"Pushing metrics: transfer_speed = {metrics['transfer_speed']} bytes/second")

            # Push to Pushgateway with error handling
            try:
                push_to_gateway(
                    PUSHGATEWAY_URL,
                    job=JOB_NAME,
                    registry=self.registry,
                    grouping_key={'instance': INSTANCE_LABEL}
                )
                logger.info(f"Successfully pushed metrics to Pushgateway at {PUSHGATEWAY_URL}")
            except Exception as push_error:
                logger.error(f"Error pushing to Pushgateway at {PUSHGATEWAY_URL}: {push_error}")

        except Exception as e:
            logger.error(f"Error preparing metrics for Prometheus: {e}")
