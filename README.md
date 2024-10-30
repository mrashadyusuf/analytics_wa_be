how to run :
- virtualenv env // create environment python for first setup
- source env/bin/activate // activate virtual environment in linux
- D:/location_env/script/activate.bat // activate virtual environment in windows
- pip install -r requirements.txt
- uvicorn main:app --reloa

update packages:
pip freeze > requirements.txt

dummy password and username:
password "admin", username: "admin"

how to deploy: 
sudo docker compose build --no-cache
sudo docker compose up -d

docs url:
(http://127.0.0.1:8000/docs)