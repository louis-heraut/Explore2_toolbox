run:
	nohup mpirun -np 5 Rscript DRIAS_export.R > mpirun_output.txt 2> mpirun_error.txt &

clean:
	rm -rf mpirun_output.txt mpirun_error.txt

tail:
	tail -f mpirun_output.txt

cat:
	cat mpirun_output.txt

find:
	ps aux | grep '[m]pirun'

kill:
	ps aux | grep '[m]pirun' | awk '{print $2}' | xargs kill
