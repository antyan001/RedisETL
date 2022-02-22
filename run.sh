#!/usr/bin/bash

KEYRINGPASS=$(cat ~/.secret/pass | sed 's/\n//g')

docker build --build-arg ssh_prv_key="$(cat ~/.ssh/id_rsa)" \
             --build-arg ssh_pub_key="$(cat ~/.ssh/id_rsa.pub)" \
             --build-arg keyring_pass="$KEYRINGPASS" \
             -f Dockerfile . -t fastapi_app #2>&1>/dev/null

## if we wanna run total rebuild and services restart -->
# docker rm -f $(docker ps -a -q) 2>&1>/dev/null
# docker run -d -p 6379:6379 docker.io/library/redis:latest /bin/sh -c 'redis-server --requirepass *****' 2>&1>/dev/null
# docker run -d -p 8001:8001 docker.io/redislabs/redisinsight:latest 2>&1>/dev/null

cd /root/ReddisPostGres/

result=`docker ps -a | grep -E "service.py|gunicorn*" | awk '{print $1}' | wc -l`
if [ $result -ge 1 ]
   then
		process_id=$(docker ps -a | grep -E "service.py|gunicorn*" | awk '{print $1}')
# 		echo 'Killing'
		for pid in $process_id; do
# 		    echo "KILL: $pid"
		    docker rm -f $pid 2>&1>/dev/null
		    sleep 1
		done
   else
        echo "docker is not running" >/dev/null
fi

docker run --device /dev/fuse \
           --cap-add SYS_ADMIN \
           --privileged \
           --env-file ./config0.env --name web_app_0 -d -p 0.0.0.0:8003:8003 docker.io/library/fastapi_app:latest
#docker run --env-file ./config1.env --name web_app_1 -d -p 0.0.0.0:8003:8003 docker.io/library/fastapi_app:latest
#docker run --env-file ./config2.env --name web_app_2 -d -p 0.0.0.0:8004:8004 docker.io/library/fastapi_app:latest


## INITIALIZE KEYRING AND MAKE A WALLET FOR USER CREDENTIALS inside the running container
docker_id="$(docker ps -a | grep -e "gunicorn" | awk '{print $1}' | xargs)"; \
docker exec -it $docker_id /bin/sh -c "cd /app/keyring/; ./set_cred_wallet.sh $KEYRINGPASS"