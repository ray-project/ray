# Updating this Lambda Function

1. Get Docker Retag via wget:
```
pushd docker/retag-lambda
wget -q https://github.com/joshdk/docker-retag/releases/download/0.0.2/docker-retag
popd
```

2. Package this folder:
```
pushd docker/retag-lambda
zip retag-lambda.zip *
```

3. Head to the AWS Management console & select the `DockerTagLatest` function. Select `Upload from`, then `.zip file` and then select the zip file created in Step 2.

