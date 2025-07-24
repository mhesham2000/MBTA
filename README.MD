# ITI-Zag Kafka Workshop - Labs

This repository contains Docker Compose configurations and lab materials for the ITI Kafka Workshop. The setup provides a complete Kafka ecosystem with monitoring, schema registry, and various connectors for hands-on learning.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose v2.x
- At least 8GB of available RAM
- Ports 8081, 8083, 8088, 8090, 9092, 9101 available

### Start the Environment
```bash
# Start all services
docker compose -f dc.yaml up -d

# Verify services are running
docker ps
```

### Stop the Environment
```bash
# Stop all services
docker compose -f dc.yaml down

# Stop and remove volumes (clean slate)
docker compose -f dc.yaml down -v
```

## 📋 Included Services

| Service | Port | Description | Access URL |
|---------|------|-------------|------------|
| **Kafka Broker** | 9092 | Apache Kafka broker with KRaft mode | `localhost:9092` |
| **Schema Registry** | 8081 | Confluent Schema Registry | `http://localhost:8081` |
| **Kafka Connect** | 8083 | Kafka Connect for data integration | `http://localhost:8083` |
| **ksqlDB Server** | 8088 | Stream processing with ksqlDB | `http://localhost:8088` |
| **Kafka UI** | 8090 | Web-based Kafka management interface | `http://localhost:8090` |
| **JMX Metrics** | 1234, 1235 | Prometheus JMX metrics endpoints | - |

## 📚 Lab Exercises

### 🔗 Kafka Fundamentals
Complete the Kafka basics lab to understand topics, producers, consumers, and partitions:
**[Kafka Lab 1](https://www.notion.so/Kafka-Lab-1-1a7b50353295800e8dffe70de2569ea0?source=copy_link)**

### 🔌 Kafka Connect Integration
Learn about data integration using Kafka Connect with various sources and sinks:
**[Kafka Connect Lab](https://www.notion.so/Kafka-Connect-Lab-1acb5035329580d78754f6efe99aa340?source=copy_link)**

## 🛠️ Configuration Details

### Resource Allocation
- **Kafka Broker**: 0.5 CPU, 2GB RAM
- **Schema Registry**: 0.5 CPU, 1GB RAM
- **Kafka Connect**: 1 CPU, 3GB RAM
- **ksqlDB Server**: 0.5 CPU, 2GB RAM
- **Kafka UI**: 0.5 CPU, 1GB RAM

### Available Volumes
- Plugin directory: `./plugins` → `/tmp/ext-plugins`
- Configuration files: `./config` → `/tmp/config`
- Miscellaneous files: `./misc` → `/tmp/misc`
- JMX monitoring: `./jmx-exporter` → `/tmp/jmx`

### JMX Monitoring
All services are configured with JMX metrics export on port 1234 for monitoring with Prometheus/Grafana.

## 🔧 Common Commands

### Kafka CLI Operations
```bash
# Access Kafka broker container
docker exec -it broker bash

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Create a topic
kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 3 --replication-factor 1

# Produce messages
kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### Schema Registry Operations
```bash
# List all schemas
curl -X GET http://localhost:8081/subjects

# Get schema by subject
curl -X GET http://localhost:8081/subjects/{subject-name}/versions/latest
```

### Kafka Connect Operations
```bash
# List connectors
curl -X GET http://localhost:8083/connectors

# Get connector status
curl -X GET http://localhost:8083/connectors/{connector-name}/status

# Delete connector
curl -X DELETE http://localhost:8083/connectors/{connector-name}
```

## 🐛 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check what's using a port
   lsof -i :9092
   
   # Kill process using port
   kill -9 <PID>
   ```

2. **Memory Issues**
   - Ensure Docker Desktop has at least 8GB RAM allocated
   - Close unnecessary applications to free up memory

3. **Docker Credential Issues**
   ```bash
   # Fix credential store issues
   rm ~/.docker/config.json
   docker login
   ```

4. **Services Not Starting**
   ```bash
   # Check logs for specific service
   docker logs -f <service-name>
   
   # Example: Check Kafka logs
   docker logs -f broker
   ```

### Health Checks
```bash


# Check Schema Registry health
curl http://localhost:8081/subjects

# Verify Connect is running
curl http://localhost:8083/connectors
```

## 🎯 Next Steps

1. Complete the [Kafka Lab 1](https://www.notion.so/Kafka-Lab-1-1a7b50353295800e8dffe70de2569ea0?source=copy_link) to understand Kafka fundamentals
2. Work through the [Kafka Connect Lab](https://www.notion.so/Kafka-Connect-Lab-1acb5035329580d78754f6efe99aa340?source=copy_link) for data integration patterns
3. Explore the Kafka UI at http://localhost:8090 to visualize your topics and messages
4. Experiment with ksqlDB for stream processing at http://localhost:8088

## 📖 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform Documentation](https://docs.confluent.io/)
- [ksqlDB Documentation](https://docs.ksqldb.io/)
- [Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)

---

**Happy Learning! 🎉**

For questions or issues, please refer to the lab instructions or consult the workshop materials.