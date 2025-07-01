workers.txt:
	aws ec2 describe-instances | grep -i publicdns | cut -d\" -f4 | uniq | grep com > workers.txt

stop:
	aws ec2 stop-instances --instance-ids `aws ec2 describe-instances | grep -i instanceid | cut -d\" -f4`

