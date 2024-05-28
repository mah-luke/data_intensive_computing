pdf:
	while true; do \
		inotifywait -e modify report.md;  \
		pandoc report.md -o report.pdf; \
	done

clean:
	ssh dic "rm -rf ~/exercise2/tmp"
	ssh dic "rm -rf ~/exercise2/src"

build:
	cd src && rm exercise2.zip && zip -r exercise2.zip exercise2 -x "*__pycache__*"

upload: build
	scp -r src dic:~/exercise2
	scp -rp src/stopwords.txt dic:~/exercise2/src/stopwords.txt
	scp pyproject.toml dic:~/exercise2

download:
	ssh dic "cat ~/exercise2/output_rdd.txt" > output_rdd.txt
	ssh dic "cat ~/exercise2/output_ds.txt" > output_ds.txt

# run_job:
# 	ssh dic "python test_hadoop.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"

# run:
# 	ssh dic "~/exercise1/src/run.sh" > out/output.txt

run-dev:
	ssh dic "/sw/venv/python39/dic24/bin/python ~/exercise1/src/run.py  --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json" > out/cluster_dev.txt

submit:
	rm -rf 32_DIC2024_Ex2
	mkdir -p 32_DIC2024_Ex2
	cp -r src 32_DIC2024_Ex1/src
	cp report.pdf 32_DIC2024_Ex2/report.pdf
	cp output_rdd.txt 32_DIC2024_Ex2/output_rdd.txt
	cp output_ds.txt 32_DIC2024_Ex2/output_ds.txt
	cd 32_DIC2024_Ex2 && zip -r ../32_DIC2024_Ex2.zip ./*
	rm -rf 32_DIC2024_Ex2

forward-hadoop:
	ssh dic -L 8080:captain01.os.hpc.tuwien.ac.at:19888

forward-dynamic:
	ssh -ND 9918 diclogin:wa

