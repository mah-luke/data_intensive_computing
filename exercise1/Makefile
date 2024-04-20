pdf:
	while true; do \
		inotifywait -e modify report.md;  \
		pandoc report.md -o report.pdf; \
	done

clean:
	ssh dic "rm -rf ~/exercise1/*"

upload:
	scp -r src dic:~/exercise1/
	# scp -rp resource/stopwords.txt dic:~/exercise1/resource/stopwords.txt

# run_job:
# 	ssh dic "python test_hadoop.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"

run:
	ssh dic "/sw/venv/python39/dic24/bin/python ~/exercise1/src/run.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -D mapreduce.job.reduces=435 -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json" > out/output.txt

# --setup 'export PYTHONPATH=$PYTHONPATH:/home/dic24/e11908553/exercise1/src/exercise1/#'
run-dev:
	ssh dic "/sw/venv/python39/dic24/bin/python ~/exercise1/src/run.py  --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json" > out/cluster_dev.txt

submit:
	rm -rf 32_DIC2024_Ex1
	mkdir -p 32_DIC2024_Ex1
	cp -r src 32_DIC2024_Ex1/src
	cp report.pdf 32_DIC2024_Ex1/report.pdf
	cp out/output.txt 32_DIC2024_Ex1/output.txt
	cd 32_DIC2024_Ex1 && zip -r ../32_DIC2024_Ex1.zip ./*
	rm -rf 32_DIC2024_Ex1

forward-hadoop:
	ssh dic -L 8080:captain01.os.hpc.tuwien.ac.at:19888

forward-dynamic:
	ssh -ND 9918 diclogin
