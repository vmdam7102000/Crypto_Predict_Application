## Running apache airflow 2.0 in docker with local executor.
Here are the steps to take to get airflow 2.0 running with docker on your machine. 
1. Clone this repo
1. Create dags, logs and plugins folder inside the project directory
```bash
mkdir ./dags ./logs ./plugins
```
1. Install docker desktop application if you don't have docker running on your machine
- [Download Docker Desktop Application for Mac OS](https://hub.docker.com/editions/community/docker-ce-desktop-mac)
- [Download Docker Desktop Application for Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows)
1. Launch airflow by docker-compose
```bash
docker-compose up -d
```
1. Check the running containers
```bash
docker ps
```
1. Open browser and type http://0.0.0.0:8080 to launch the airflow webserver

![](images/screenshot_airflow_docker.png)