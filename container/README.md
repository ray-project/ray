# README

## Build
```bash
cd ray
docker build -t ray-sea2bel-collective .
docker images | grep ray-sea2bel-collective
docker tag ray-sea2bel-collective:latest dengwxn/ray-sea2bel-collective:latest
docker push dengwxn/ray-sea2bel-collective:latest
```

## Run
```bash
docker run -d --name ray-sea2bel-collective-run \
  --shm-size=128gb \
  --gpus all \
  --cap-add SYS_PTRACE \
  -v $(pwd):/app \
  -it ray-sea2bel-collective
docker exec -it ray-sea2bel-collective-run zsh
```

## Clean
```bash
docker stop ray-sea2bel-collective-run
docker rm ray-sea2bel-collective-run
```

## Install
```bash
cd /app/container
./install_container.sh
```
