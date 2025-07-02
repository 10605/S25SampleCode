workers.txt:
	aws ec2 describe-instances | grep -i publicdns | cut -d\" -f4 | uniq | grep com > workers.txt

stop:
	aws ec2 stop-instances --instance-ids `aws ec2 describe-instances | grep -i instanceid | cut -d\" -f4`

# tests

wc_nano_map_only:
	python3 -m fire wc_nano.py WordCount map_only ../data/bluecorpus.txt bluecounts.txt 
	grep -w this bluecounts.txt | head -10

wc_nano:
	python3 -m fire wc_nano.py WordCount map_reduce ../data/bluecorpus.txt bluecounts.txt 
	grep -w this bluecounts.txt | head -10

wc_micro_map_only:
	python3 -m fire wc_micro.py WordCount map_only ../data/bluecorpus.txt bluecounts.txt 
	grep -w this bluecounts.txt | head -10

wc_micro:
	python3 -m fire wc_micro.py WordCount map_reduce ../data/bluecorpus.txt bluecounts.txt 
	grep -w this bluecounts.txt | head -10
