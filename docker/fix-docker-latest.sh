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

ASSUME_ROLE_CREDENTIALS=$(aws sts assume-role --role-arn arn:aws:iam::"$(aws sts get-caller-identity | jq -r .Account)":role/InvokeDockerTagLatest --role-session-name push_latest)

AWS_ACCESS_KEY_ID=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.AccessKeyId)
AWS_SECRET_ACCESS_KEY=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.SecretAccessKey)
AWS_SESSION_TOKEN=$(echo "$ASSUME_ROLE_CREDENTIALS" | jq -r .Credentials.SessionToken)

echo -e "Invoking this lambda!\nView logs at https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups"
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN AWS_SECURITY_TOKEN='' aws \
lambda invoke --function-name DockerTagLatest \
--cli-binary-format raw-in-base64-out \
--cli-read-timeout 600 \
--payload "{\"source_tag\" : \"$IMAGE\", \"destination_tag\" : \"latest\"}" /dev/stdout

echo -e "Please check logs before rerunning!!!!\n\nAt the time of writing, Autoscaler Images are not built.\nSo retagging errors for those images are expected!"
