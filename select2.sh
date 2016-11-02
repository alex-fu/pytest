python -m mysql_test.t2 -d t1 -c 120 -t 50000 select &
python -m mysql_test.t2 -d t2 -c 120 -t 50000 select &
python -m mysql_test.t2 -d t3 -c 120 -t 50000 select &
python -m mysql_test.t2 -d t4 -c 120 -t 50000 select &
