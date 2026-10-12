docker network create tp-network

docker build -t microservice-clients ./clients
docker build -t microservice-commandes ./commandes

docker run -d --name clients --network tp-network -p 8080:8080 microservice-clients
docker run -d --name commandes --network tp-network -p 8081:8080 -e CLIENTS_URL=http://clients:8080 microservice-commandes

docker ps
pause
curl.exe http://localhost:8080/clients
curl.exe http://localhost:8081/commandes
curl.exe http://localhost:8081/commandes/1
pause