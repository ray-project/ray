#!/bin/bash

IMAGE="1.7.0"

if [ $# -eq 0 ]
then
echo "Please Specify the release tag (i.e. 1.7.0)"
exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --release-tag)
    shift
    IMAGE=$1
    ;;
    *)
    echo "Usage: fix-docker-latest.sh --release-tag <TAG>"
    exit 1
esac
shift
done

<<<<<<< HEAD
echo "You must be logged into a user with push privileges to do this."
for REPO in "ray" "ray-ml"
do
    while IFS= read -r PYVERSION; do
      while IFS= read -r CUDAVERSION; do
        export SOURCE_TAG="$IMAGE"-"$PYVERSION"-"$CUDAVERSION"
        export DEST_TAG="$DEST"-"$PYVERSION"-"$CUDAVERSION"
        docker pull rayproject/"$REPO":"$SOURCE_TAG"
        docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"
        docker push rayproject/"$REPO":"$DEST_TAG"
      done < cuda_versions.txt

      export SOURCE_TAG="$IMAGE"-"$PYVERSION"
      export DEST_TAG="$DEST"-"$PYVERSION"
      docker pull rayproject/"$REPO":"$SOURCE_TAG"
      docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"
      docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"-cpu

      docker pull rayproject/"$REPO":"$SOURCE_TAG"-gpu
      docker tag rayproject/"$REPO":"$SOURCE_TAG"-gpu rayproject/"$REPO":"$DEST_TAG"-gpu

      docker push rayproject/"$REPO":"$DEST_TAG"
      docker push rayproject/"$REPO":"$DEST_TAG"-cpu
      docker push rayproject/"$REPO":"$DEST_TAG"-gpu
    done < python_versions.txt
done
=======
ASSUME_ROLE_CREDENTIALS=$(aws sts assume-role --role-arn arn:aws:iam::"$(aws sts get-caller-identity | jq -r .Account)":role/InvokeDockerTagLatest --role-session-name push_latest)
>>>>>>> 15ca575078b79163ce8cc0d4e72ea38d45eb69b1

AWS_ACCESS_KEY_ID=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.AccessKeyId)
AWS_SECRET_ACCESS_KEY=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.SecretAccessKey)
AWS_SESSION_TOKEN=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.SessionToken)

<<<<<<< HEAD
for REPO in "ray" "ray-ml" "ray-deps" "base-deps"
do
    while IFS= read -r CUDAVERSION; do
      export SOURCE_TAG="$IMAGE"-"$CUDAVERSION"
      export DEST_TAG="$DEST"-"$CUDAVERSION"
      docker pull rayproject/"$REPO":"$SOURCE_TAG"
      docker tag rayproject/"$REPO":"$SOURCE_TAG" rayproject/"$REPO":"$DEST_TAG"
      docker push rayproject/"$REPO":"$DEST_TAG"
    done < cuda_versions.txt

    docker pull rayproject/"$REPO":"$IMAGE"
    docker tag rayproject/"$REPO":"$IMAGE" rayproject/"$REPO":"$DEST"
    docker tag rayproject/"$REPO":"$IMAGE" rayproject/"$REPO":"$DEST"-cpu

    docker pull rayproject/"$REPO":"$IMAGE"-gpu
    docker tag rayproject/"$REPO":"$IMAGE"-gpu rayproject/"$REPO":"$DEST"-gpu
    
    docker push rayproject/"$REPO":"$DEST"
    docker push rayproject/"$REPO":"$DEST"-cpu
    docker push rayproject/"$REPO":"$DEST"-gpu
done
=======


echo -e "Invoking this lambda!\nView logs at https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups"
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN AWS_SECURITY_TOKEN='' aws \
lambda invoke --function-name DockerTagLatest \
--cli-binary-format raw-in-base64-out \
--cli-read-timeout 600 \
--payload "{\"source_tag\" : \"$IMAGE\", \"destination_tag\" : \"latest\"}" /dev/stdout

echo -e "Please check logs before rerunning!!!!\n\nAt the time of writing Ray-ML/Autoscaler Images are not built for Py39\nSo retagging errors for those images are expected!"
>>>>>>> 15ca575078b79163ce8cc0d4e72ea38d45eb69b1
