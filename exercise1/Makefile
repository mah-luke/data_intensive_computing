pdf:
	while true; do \
		inotifywait -e modify report.md;  \
		pandoc report.md -o report.pdf; \
	done

cluster:
	ssh dic "rm -rf ~/exercise1/*"
	scp -r src dic:~/exercise1/
	scp resource/stopwords.txt dic:~/exercise1/resource/stopwords.txt

run_job:
	ssh dic "python test_hadoop.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"
