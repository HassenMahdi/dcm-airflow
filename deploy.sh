ssh datacapture@51.103.23.198
sudo su
cd /project_datacapture/dcm-airflow/
docker-compose down
git pull
docker-compose up --build --force-recreate -d