# SmartQueue: An Asynchronous Task Queue System

SmartQueue is a robust, asynchronous task queue built with Python, FastAPI, and Redis. It is designed for high performance and resilience, featuring multiple scheduling strategies, automatic retries with exponential backoff, and concurrent task processing.

This project is fully containerized using Docker for easy setup and deployment.



---

## ✨ Features

-   **Asynchronous Task Processing:** Offload long-running tasks from your main application.
-   **Concurrent Workers:** Utilizes a `ThreadPoolExecutor` to process multiple tasks in parallel, maximizing throughput.
-   **Multiple Scheduling Strategies:** Easily configure the worker to use different schedulers:
    -   **Priority:** Processes tasks with higher priority scores first.
    -   **Round Robin:** Distributes tasks evenly across multiple queues.
    -   **Weighted Round Robin:** Polls queues based on predefined weights.
-   **Automatic Retries:** Failed tasks are automatically retried with exponential backoff to handle transient errors gracefully.
-   **Dead-Letter Queue:** Tasks that fail permanently are moved to a DLQ for manual inspection.
-   **Dockerized:** All components (API, Worker, Redis) are managed by Docker Compose for a one-command setup.

---

## 🛠️ Tech Stack

-   **Backend:** Python
-   **API:** FastAPI
-   **Queue/Broker:** Redis
-   **Concurrency:** `concurrent.futures.ThreadPoolExecutor`
-   **Containerization:** Docker & Docker Compose

---

## 🚀 Getting Started

### Prerequisites

-   Docker
-   Docker Compose

### Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd smartqueue
    ```

2.  **Build and run the services:**
    This single command will build the containers and start the API, worker, and Redis.
    ```bash
    docker-compose up --build
    ```
    -   The API will be available at `http://localhost:8000`.
    -   Interactive API docs (Swagger UI) are at `http://localhost:8000/docs`.

### Configuring the Worker

You can change the worker's scheduling strategy by editing the `SCHEDULING_STRATEGY` environment variable in the `docker-compose.yml` file.

**Options:**
-   `priority` (default)
-   `round_robin`
-   `weighted_round_robin`

---

## 📝 API Usage

Submit a task by sending a `POST` request to the `/submit` endpoint.

**Example using cURL:**

```bash
curl -X POST "http://localhost:8000/submit" \
-H "Content-Type: application/json" \
-d '{
  "task_name": "send_welcome_email",
  "priority": 8,
  "payload": {
    "email": "test@example.com",
    "name": "Alex"
  }
}'