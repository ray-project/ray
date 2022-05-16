# Updating this Lambda Function

1. Get Docker Retag via wget:
```
pushd docker/retag-lambda
wget -q https://github.com/joshdk/docker-retag/releases/download/0.0.2/docker-retag
popd
```

2. Package this folder:
```
pushd docker
zip -r retag-lambda.zip retag-lambda/
```

3. Upload zip file to update the code.

