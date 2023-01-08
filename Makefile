get-clickhouse-docker-image:
	docker pull clickhouse/clickhouse-server

start-clickhouse-docker-container:
	docker run -d -p 18123:8123 -p19000:9000 --name clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server

stop-clickhouse-docker-container:
	docker stop clickhouse-server && docker rm clickhouse-server

start-kafka-server:
	docker-compose -f kafka-docker-compose.yaml up

stop-kafka-server:
	docker-compose -f kafka-docker-compose.yaml down
