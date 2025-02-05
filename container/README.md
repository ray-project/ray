# README

## Build
```bash
cd ray
docker build -t ray-sea2bel .
docker images | grep ray-sea2bel
docker tag ray-sea2bel:latest dengwxn/ray-sea2bel:latest
docker push dengwxn/ray-sea2bel:latest
```

## Run
```bash
docker run -d --name ray-sea2bel-dev \
  --shm-size=128gb \
  --gpus all \
  --cap-add SYS_PTRACE \
  -v $(pwd):/app \
  -it ray-sea2bel
docker exec -it ray-sea2bel-dev zsh
```

## Clean
```bash
docker stop ray-sea2bel-dev
docker rm ray-sea2bel-dev
```

## Install
```bash
cd /app/container
./install.sh
```
