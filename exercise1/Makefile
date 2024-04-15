pdf:
	while true; do \
		inotifywait -e modify report.md;  \
		pandoc report.md -o report.pdf; \
	done


