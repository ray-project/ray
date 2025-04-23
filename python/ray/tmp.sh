for region in $(aws ec2 describe-regions --query "Regions[].RegionName" --output text); do
  echo "Region: $region";
  aws ec2 describe-images \
    --region $region \
    --filters "Name=name,Values=Deep Learning AMI (Ubuntu 18.04) Version 71.0*" \
    --query "Images[].[ImageId, Name]" \
    --output text;
  echo "--------------------------";
done
