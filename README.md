how to run :
pip install -r requirements.txt
uvicorn main:app

dummy password and username:
password "admin", username: "admin"

how to deploy: 
sudo docker compose build --no-cache
sudo docker compose up -d