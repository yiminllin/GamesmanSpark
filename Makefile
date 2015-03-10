MASTER="local[8]"
SOLVER=SparkBeta.py

.PHONY: clean

default: beta

beta:
	rm -rf output.txt
	PYTHONWARNINGS="ignore" time spark-submit $(SOLVER) --master=$(MASTER)

test:
		PYTHONWARNINGS="ignore" time spark-submit SparkTest.py

clean:
	rm -rf *.pyc
