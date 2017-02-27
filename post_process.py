import os
import argparse
import numpy as np
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser(
        description='post process logs')
parser.add_argument('-i', '--input', help="input processed log", required=True)
parser.add_argument('-t', '--title',
                        help='test dataset', default="no title")
parser.add_argument('-s', '--save-fig', default=None)
args = vars(parser.parse_args())


schedular_delay = []
task_deserialization = []
hdfs_read =[]
local_read_wait = []
network_Wait = []
data_deserilization = []
output_write_wait = []
compute = []
gc = []

identifier_mapping ={
3:"COMPUTE",  
4:"GC",
6:"SCHEDULAR_DELAY",
8:"TASK_DESERIAL",
7: "HDFS_READ",
1:"LOCAL_READ_WAIT",
2: "NETWORK_WAIT",
9:"DATA_DESERIALIZATION",
5: "OUTPUT_WRITE_WAIT"
}
COMPUTE = 3
GC = 4
SCHEDULAR_DELAY = 6
TASK_DESERIAL = 8
HDFS_READ = 7
LOCAL_READ_WAIT = 1
NETWORK_WAIT = 2
DATA_DESERIALIZATION = 9
OUTPUT_WRITE_WAIT = 5

transformation_mapping = {
COMPUTE: compute, GC: gc, 
SCHEDULAR_DELAY: schedular_delay, TASK_DESERIAL:task_deserialization, 
HDFS_READ: hdfs_read, LOCAL_READ_WAIT: local_read_wait, NETWORK_WAIT:network_Wait,
DATA_DESERIALIZATION: data_deserilization, OUTPUT_WRITE_WAIT: output_write_wait
}
#set arrow from 0,0 to 203,0 ls 6 nohead
transform = lambda x:float(x)
input_file = args['input']
with open(input_file) as f:
	for line in f:
		line = line.rstrip(os.linesep)
		if "set arrow from" in line:
			chunks = line.split()
			#print chunks
			x_start,y_start = map(transform ,chunks[3].split(","))
			x_end, y_end = map(transform, chunks[5].split(","))
			identifier = int(chunks[7])

			#print x_start, y_start, x_end, y_end
			time_spent = x_end - x_start
			data_list = transformation_mapping[identifier]
			data_list.append(time_spent)
			transformation_mapping[identifier] = data_list
f.close()
summation = lambda l:sum(l)
IO = [transformation_mapping[HDFS_READ], transformation_mapping[DATA_DESERIALIZATION],
	 transformation_mapping[OUTPUT_WRITE_WAIT], transformation_mapping[LOCAL_READ_WAIT]]
IO_FINAL = [sum(map(summation, IO)),0]
CPU = [transformation_mapping[COMPUTE]]
CPU_FINAL = [sum(map(summation, CPU)),0]
NETWORK = [transformation_mapping[NETWORK_WAIT], transformation_mapping[SCHEDULAR_DELAY]]
NETWORK_FINAL = [sum(map(summation, NETWORK)),0]
OVERHEADS = [transformation_mapping[GC], transformation_mapping[TASK_DESERIAL]]
OVERHEADS_FINAL = [sum(map(summation, OVERHEADS)),0]
print IO_FINAL, CPU_FINAL, NETWORK_FINAL, OVERHEADS_FINAL

N = 1
ind = np.arange(2)    
width = 0.3     

p1 = plt.bar(ind, IO_FINAL, width=width, color='r')
p2 = plt.bar(ind, CPU_FINAL, width= width, color='y', bottom=IO_FINAL)
p3 = plt.bar(ind, NETWORK_FINAL, width=width, color='b',bottom=[IO_FINAL[j] +CPU_FINAL[j] for j in range(len(IO_FINAL))])
p4 = plt.bar(ind, OVERHEADS_FINAL, width= width, color='g',bottom=[IO_FINAL[j] +CPU_FINAL[j] +NETWORK_FINAL[j] for j in range(len(IO_FINAL))])

plt.ylabel('Time')
plt.title(args['title'])
plt.xticks(ind+width/2., ('Run Times',) )
plt.legend( (p1[0], p2[0], p3[0], p4[0]), ('IO', 'CPU', 'NETWORK','OVERHEADS'))
if args['save_fig']:
	plt.savefig(args['save_fig'],dpi=150)
plt.show()
