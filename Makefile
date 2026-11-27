.PHONY: test cli-help up-4x4 up-3x2 run-4x4 run-3x2 down

test:
	lein test

cli-help:
	lein run test --help

up-4x4:
	./scripts/up.sh 4x4

up-3x2:
	./scripts/up.sh 3x2

run-4x4:
	./scripts/run-suite.sh 4x4

run-3x2:
	./scripts/run-suite.sh 3x2

down:
	./scripts/down.sh
