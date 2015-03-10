MASTER="local[8]"
SOLVER=SparkBeta.py

.PHONY: clean

default: beta

beta:
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) --master=$(MASTER)

test:
		PYTHONWARNINGS="ignore" time spark-submit als.py

clean:
	rm -rf *.pyc
