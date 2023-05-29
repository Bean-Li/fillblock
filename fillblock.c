#define _GNU_SOURCE
#define _DEFAULT_SOURCE 
#define _XOPEN_SOURCE 600

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <malloc.h>

#define MB (1024*1024)
#define GB (1024*1024*1024)

char devicename[1024];
ssize_t wsize = 0;
ssize_t buf_sz = 0;
int thread_num  = 0;
int g_interval = 0 ;
int verbose_flag = 0;



void usage()
{
    fprintf(stdout, "fillblock usage:\n");
	fprintf(stdout, "==================\n");
	fprintf(stdout, " -d [devicename]  specify the block device you want to fill\n");
	fprintf(stdout, " -p [thread_num]  specify the thread num you want employ to fill the block\n");
	fprintf(stdout, " -s [size]        specify the size you want to write, [0, size-1] will filled\n");
	fprintf(stdout, " -b [buffer_size] specify the buffer size of every write operation\n");
	fprintf(stdout, " -i [interval]    output statistics every [interval] second\n");
	fprintf(stdout, " -v [verbose]     if v == 2 , output latency historgram statistics every [interval] second\n");
}

ssize_t parse_space_size(char* inbuf)
{
	ssize_t out_size = 0 ;
	char *p_res = NULL ;

	out_size = strtol(inbuf, &p_res, 10) ;
	if(p_res == NULL)
	{
		return out_size ;
	}

	switch(*p_res)
	{
	case 'k':
	case 'K':
		out_size *= 1024;
		break;
	case 'm':
	case 'M':
		out_size *= (1024*1024);
		break;
	case 'g':
	case 'G':
		out_size *= (1024*1024*1024);
		break;
	case 't':
	case 'T':
		out_size *= (long)(1024*1024*1024)*1024;
		break;
	default:
		break;
	}
	return out_size ;
}

int HIST_INTERVAL[] = {0,10,50,100,200,500,
	1000,2000,3000,4000,5000,6000,7000,8000,9000,
	10000,20000,40000,60000,80000,
	100000,200000,300000,400000,500000,600000,700000,800000,900000,
	1000000,2000000,4000000,8000000};

#define  N_HIST  (sizeof(HIST_INTERVAL)/sizeof(int)) 

struct latency_stat {
	unsigned long long op_times ;
	unsigned long long total ;
	unsigned long long min ;
	unsigned long long max ;
	unsigned long long hist_array[N_HIST];
}; 

struct operation_stat{
	unsigned long long write_success ; 
	unsigned long long write_bytes ;
	unsigned long long write_fail;

	struct latency_stat write_l_stat ;
	char padding[40];
} __attribute__((__aligned__(64)));

struct operation_stat**  statistic;

struct operation_stat last_statistic ; 

static inline void __init_latency_stat(struct latency_stat* l_stat)
{
	l_stat->op_times = 0;
	l_stat->total = 0;
	l_stat->min = 0 ;
	l_stat->max =0;
	int i = 0; 
	for(i = 0 ; i < N_HIST ; i++ )
	{
		l_stat->hist_array[i] = 0;
	}
}

static inline void update_latency_stat(struct latency_stat* l_stat, unsigned long long latency)
{
	l_stat->op_times++ ;
	l_stat->total += latency ;

	if(l_stat->min == 0 || l_stat->min > latency)
		l_stat->min = latency ;

	if(l_stat->max < latency)
		l_stat->max = latency ;

	int i = 0 ; 
	for(i = 1; i < N_HIST; i++)
	{
		if(latency <= HIST_INTERVAL[i])
		{
			l_stat->hist_array[i-1] += 1;
			break;
		}
	}
	if( i == N_HIST)
	{
		l_stat->hist_array[i-1] += 1 ;
	}
}

static inline void __summary_latency_stat(struct latency_stat* result, struct latency_stat* part)
{
	result->op_times += part->op_times ;
	result->total += part->total;

	if(result->min == 0 || result->min > part-> min)
	{
		result->min = part->min ;
	}

	if(result->max < part->max)
	{
		result->max = part->max ;
	}

	int i ; 
	for( i = 0; i < N_HIST; i++)
	{
		result->hist_array[i] += part->hist_array[i];
	}
}


static inline void print_latency_stat(struct latency_stat* l_stat)
{
	if(l_stat->op_times == 0)
	{
		fprintf(stdout, "     avg: %-8d  min: %-8d  max: %-8d\n", 0,0,0);
	}
	else
	{
		fprintf(stdout, "     avg: %-8llu  min: %-8llu  max: %-8llu\n", 
				l_stat->total/l_stat->op_times,l_stat->min,l_stat->max);
		int i ;
		unsigned long long cum = 0 ;

		fprintf(stdout, "-----------------------------HISTGRAM---------------------------------------\n");
		for(i = 0; i < N_HIST ; i++)
		{
			if(cum >= l_stat->op_times)
			{
				break ;
			}
			cum += (l_stat->hist_array[i]) ; 
			if(cum == 0)
			{
				continue;
			}
			if(i < (N_HIST - 1) &&  HIST_INTERVAL[i+1] < 1000)
			{
				fprintf(stdout, "%6d(us)~%6d(us): %8llu %6.2f%%  %6.2f%% \n",
						HIST_INTERVAL[i],HIST_INTERVAL[i+1], l_stat->hist_array[i],
						(float)(100*l_stat->hist_array[i])/(l_stat->op_times), (float)(100*cum)/(l_stat->op_times));
			}
			else if(i < N_HIST - 1 )
			{
				fprintf(stdout, "%6d(ms)~%6d(ms): %8llu %6.2f%%  %6.2f%% \n",
						HIST_INTERVAL[i]/1000,HIST_INTERVAL[i+1]/1000, l_stat->hist_array[i],
						(float)(100*l_stat->hist_array[i])/(l_stat->op_times), (float)(100*cum)/(l_stat->op_times));
			}
			else
			{
				fprintf(stdout, "%6d(ms)~          : %8llu %6.2f%%  %6.2f%% \n",
						HIST_INTERVAL[i]/1000, l_stat->hist_array[i],
						(float)(100*l_stat->hist_array[i])/(l_stat->op_times), (float)(100*cum)/(l_stat->op_times));
			}
		}
		fprintf(stdout, "----------------------------------------------------------------------------\n");
	}
}

static void print_realtime_latency_stat(char* op_type, struct latency_stat* current, struct latency_stat* last)
{
	struct latency_stat this_loop ;
	this_loop.op_times = current->op_times - last->op_times;

	if (this_loop.op_times == 0 )
		return ;
	if (verbose_flag  != 2 )
	{
		return ;
	}

	this_loop.total = current->total - last->total;
	float avg = this_loop.total / this_loop.op_times;

	int i ;

	fprintf(stdout, "-----------------%-6s HISTGRAM-------------------------\n", 
			op_type);
	fprintf(stdout, "(avg = %6.2f, total = %8llu)\n", 
			avg, this_loop.op_times);
	unsigned long long cum = 0 ;
	unsigned long long this_times = 0;
	for(i = 0; i < N_HIST ; i++)
	{
		if (cum >= this_loop.op_times)
			break;
		this_times = current->hist_array[i] - last->hist_array[i];
		if(this_times ==0)
			continue;
		cum += this_times;

		if(i < (N_HIST - 1) &&  HIST_INTERVAL[i+1] < 1000)
		{
			fprintf(stdout, "%6d(us)~%6d(us): %8llu %6.2f%%  %6.2f%% \n",
					HIST_INTERVAL[i],HIST_INTERVAL[i+1], this_times,
					(float)(100*this_times)/(this_loop.op_times), (float)(100*cum)/(this_loop.op_times));
		}
		else if(i < N_HIST - 1 )
		{
			fprintf(stdout, "%6d(ms)~%6d(ms): %8llu %6.2f%%  %6.2f%% \n",
					HIST_INTERVAL[i]/1000,HIST_INTERVAL[i+1]/1000, this_times,
					(float)(100*this_times)/(this_loop.op_times), (float)(100*cum)/(this_loop.op_times));
		}
		else
		{
			fprintf(stdout, "%6d(ms)~          : %8llu %6.2f%%  %6.2f%% \n",
					HIST_INTERVAL[i]/1000, this_times,
					(float)(100*this_times)/(this_loop.op_times), (float)(100*cum)/(this_loop.op_times));

		}

	}
	fprintf(stdout, "-----------------------------------------------------------------------------------\n");
}
static void init_latency_stat(struct operation_stat* stat)
{
	__init_latency_stat(&(stat->write_l_stat));
}

static void summary_latency_stat(struct operation_stat* stat , struct operation_stat* part)
{
	__summary_latency_stat(&(stat->write_l_stat), &(part->write_l_stat));
}

int init_statistic(struct operation_stat* stat)
{
	stat->write_success    = 0 ;
	stat->write_bytes      = 0;
	stat->write_fail       = 0;
	init_latency_stat(stat);
	return 0;
}

static int print_statistic(struct operation_stat* stat)
{
	fprintf(stdout, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	{
		/*skip_dir = 1 mean skip create folder*/
		fprintf(stdout, "write_success   : %12llu ", stat->write_success);
		print_latency_stat(&(stat->write_l_stat));
		fprintf(stdout, "write_fail      : %12llu\n", stat->write_fail);
	}
	fprintf(stdout, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
	return 0;

}

static void print_statistic_summary()
{
	int i ; 
	struct operation_stat total_stat;
	init_statistic(&total_stat);

	for(i = 0 ; i < thread_num ; i++)
	{
		total_stat.write_success    += statistic[i]->write_success ;
		total_stat.write_fail       += statistic[i]->write_fail ;
		summary_latency_stat(&total_stat, statistic[i]);

	}

	print_statistic(&total_stat) ;
	return ;
}


static void __print_realtime_stat(struct operation_stat* current, struct operation_stat* last)
{
	int write_ops = (current->write_success - last->write_success)/g_interval;
	ssize_t write_bw = (current->write_bytes - last->write_bytes)/g_interval ; 

	time_t ltime = time(NULL); 
	struct tm now_time ; 
	localtime_r(&ltime, &now_time);

	char buf[100];
	if(write_ops >0)
	{

		strftime(buf, 100, "%Y-%m-%d %H:%M:%S", &now_time);
		fprintf(stdout, "%s", buf);
		char w_bw_buffer[256] ; 
		if(write_bw < 1024)
		{
			snprintf(w_bw_buffer, 256, "%8zd  Bps", write_bw);
		}
		else if( write_bw > 1024 && write_bw < MB)
		{
			float write_bw_kb = (1.0 * write_bw/1024);
			snprintf(w_bw_buffer,256, "%8.2f KBps", write_bw_kb);
		}
		else if(write_bw > MB && write_bw < GB)
		{
			float write_bw_mb = (1.0 * write_bw/1024/1024);
			snprintf(w_bw_buffer,256, "%8.2f MBps", write_bw_mb);
		}
		else 
		{
			float write_bw_gb = (1.0 * write_bw/1024/1024/1024);
			snprintf(w_bw_buffer, 256, "%8.2f GBps", write_bw_gb);
		}
		float progress_ratio = (100.0 * current->write_bytes / wsize);
		fprintf(stdout, " %8d write op/s  write_bw %s  finished %7.3f%%\n", 
			    write_ops, w_bw_buffer,progress_ratio) ; 
		print_realtime_latency_stat("write", &(current->write_l_stat), &(last->write_l_stat));
	}

	memcpy(last, current,sizeof(struct operation_stat)) ;
}
static void print_realtime_stat()
{
	int i ; 
	struct operation_stat total_stat;
	init_statistic(&total_stat);

	for(i = 0 ; i < thread_num ; i++)
	{
		total_stat.write_success    += statistic[i]->write_success ;
		total_stat.write_bytes      += statistic[i]->write_bytes ;
		total_stat.write_fail       += statistic[i]->write_fail ;
		summary_latency_stat(&total_stat, statistic[i]);
	}

	__print_realtime_stat(&total_stat, &last_statistic);
}

unsigned long long time_us()                                                                                                                           
{
	struct timeval tv ; 
	gettimeofday(&tv, NULL);

	return (unsigned long long)(tv.tv_sec*1000000 + tv.tv_usec);
}


void signal_handler(int signo)
{
	switch(signo)
	{
	case SIGINT:
		exit(130);
		break;
	case SIGUSR1:
		print_statistic_summary();
		break;
	}
}   


int r_write(int fd, char* buffer, ssize_t size)
{
	int reserve_bytes = size;
	int write_bytes = 0;
	char errmsg[1024];

	while(reserve_bytes > 0)
	{
		write_bytes = write(fd, buffer, reserve_bytes);
		if(write_bytes >= 0)
		{
			reserve_bytes -= write_bytes;
			buffer += write_bytes;
		} 
		else if(write_bytes <0 && errno != EINTR)
		{
			memset(errmsg, '\0', 1024);
			strerror_r(errno, errmsg, sizeof(errmsg));
			fprintf(stderr, "error happened when write (errno %d %s)\n", errno, errmsg);
			return -1;
		}
	}

	if (reserve_bytes == 0)
		return size;
	else
		return -1;
}


void * work_thread(void* param)                                                                                                                        
{
	int idx = (unsigned long)(param);
	char errmsg[1024];

	if(idx < 0 || idx >= thread_num) 
	{ 
		fprintf(stderr,"invalidate work thread idx:%d\n", idx); 
		return NULL; 
	}

	struct operation_stat *stat = statistic[idx];
    ssize_t block_cnt = (wsize + buf_sz - 1)/buf_sz ; 
	ssize_t block_every = (block_cnt + (thread_num - 1)) / thread_num;


	ssize_t start = block_every * idx * buf_sz ; 
	ssize_t end   = block_every * buf_sz * (idx + 1); 
	if (end > wsize)
		end = wsize ; 

    int fd = open(devicename, O_WRONLY|O_DIRECT);
	if (fd < 0 )
	{
        fprintf(stderr, "failed to open %s\n", devicename);
		return NULL;
	}

	lseek(fd, start,  SEEK_SET);
	char * buffer = memalign(4096, buf_sz);
	if(buffer == NULL)
	{
	    fprintf(stderr, "failed to malloc buffer with size %ld\n", buf_sz);
		return NULL ;
	}
	memset(buffer, 'c', buf_sz) ;
	memset(errmsg, '\0', 1024) ;
	ssize_t length = 0;
	ssize_t write_bytes;
	ssize_t current_size ; 
	unsigned long long begin_us , end_us ; 
	while(length < (end - start))
	{
		if(end - start - length > buf_sz)
			current_size = buf_sz;
		else
			current_size = end - start - length ;

		begin_us = time_us();
		write_bytes = r_write(fd, buffer, current_size);

		if(write_bytes > 0)
		{
			end_us = time_us();
			stat->write_success++;
			stat->write_bytes += write_bytes;
			length += write_bytes;
			update_latency_stat(&(stat->write_l_stat), end_us-begin_us);
		}
		else
		{
			stat->write_fail++;
			sleep(1000);
			strerror_r(errno, errmsg, sizeof(errmsg));
			fprintf(stderr, "Thread-%d failed to write at offset %ld~%ld (errno %d: %s)\n",
					idx, start+length,current_size, errno, errmsg);
			break;
		}

	}

	return NULL ;
	
}


int main(int argc , char* argv[])
{
	int ch;
	int option_index = 0 ;

	int ret ; 
	int i ; 

	char errmsg[1024];
	struct stat statbuf;

	static struct option long_options[] = {
		{"devicename",       required_argument, 0, 'd'},
		{"parallel",         required_argument, 0, 'p'},
		{"writesize",        required_argument, 0, 's'},
		{"buffersize",       required_argument, 0, 'b'},
		{"interval",         required_argument, 0, 'i'},
		{"verbose",          required_argument, 0, 'v'},
		{0, 0 , 0, 0}
	};

	memset(devicename,'\0', 1024);
	memset(errmsg,'\0', 1024);

	while((ch = getopt_long(argc, argv, "h?d:p:s:b:i:v:", long_options, &option_index)) != -1)
	{
		switch(ch)
		{
		case 'd':
			if (strncmp(optarg,"/dev/", 5) ==0)
			{
			    strcpy(devicename, optarg);
			}
			else
			{
			    fprintf(stderr, "invalid device name %s , it should start with /dev/\n", optarg);
				exit(1);
			}

		    ret = stat(devicename, &statbuf);
			if(ret != 0)
			{
				strerror_r(errno, errmsg, sizeof(errmsg));
			    fprintf(stderr, "failed to stat %s %d: (%s)\n", devicename, errno, errmsg);
				exit(1);
			}
			if(!S_ISBLK(statbuf.st_mode))
			{
				fprintf(stderr, "%s is not block device\n",devicename);
				exit(1);
			}
			break;

		case 'p':
			thread_num = atoi(optarg);
			break;

		case 's':
			wsize = parse_space_size(optarg);
			if(wsize < 0)
			{
				fprintf(stderr, "invalid filesize specified(%s)\n", optarg);
				exit(1);
			}
			break; 

		case 'b':
			buf_sz = parse_space_size(optarg);
			if(ret != 0)
			{
				fprintf(stderr, "invalid buffer size specified (%s)\n", optarg);
				exit(1);
			}
			break;

		case 'i':
			g_interval = atoi(optarg) ;
			break ;  

		case 'v': 
			verbose_flag = atoi(optarg);
			break;

		case 'h':
		case '?':
			usage();
			return 0;
			break;
		default:
			break;

		}
	}

	if(thread_num < 0)
	{
		fprintf(stderr,"Invalid thread num \n");
		exit(1);
	}

	if(strlen(devicename) == 0)
	{
		fprintf(stderr, "you should specify a block device first\n");
		exit(2);
	}
	if(buf_sz == 0)
	{
	    buf_sz = 4 * 1024 * 1024 ;
	}
	if(wsize == 0)
	{
		int fd=open(devicename,O_RDONLY);
		if(fd < 0)
		{
		    fprintf(stderr, "failed to open %s\n", devicename);
			exit(2);
		}
		if (ioctl(fd,BLKGETSIZE64,&wsize)==-1) 
		{
			fprintf(stderr, "failed to exec ioctl BLKGETSIZE64 on %s\n", devicename);
			exit(2);
		}
		close(fd);
	}

	statistic = malloc(thread_num * sizeof(struct operation_stat*));
	if(statistic == NULL)
	{
		fprintf(stderr, "failed to malloc statistic\n");
		exit(2);
	}

	for(i = 0 ; i < thread_num ; i++)
	{
		statistic[i] = (struct operation_stat*) malloc(sizeof(struct operation_stat));
		if(statistic[i] == NULL)
		{
			fprintf(stderr, "failed to malloc statistic[%d]\n", i);
			exit(2);
		}
		init_statistic(statistic[i]);
	}

	pthread_t *tid_array = (pthread_t*) malloc(thread_num * sizeof(pthread_t));
	unsigned long idx ;  
	for(i = 0; i < thread_num; i++)
	{
		idx = i ; 
		ret = pthread_create(&(tid_array[i]), NULL, work_thread, (void*) idx);
		if(ret !=0 )
		{
			strerror_r(errno, errmsg, sizeof(errmsg));
			fprintf(stderr, "failed to create thread %d (%s)\n", i, errmsg);
			exit(2);
		}
	}

	struct sigaction new_action, old_action;
	new_action.sa_handler = signal_handler;
	new_action.sa_flags = 0 ; 
	sigemptyset(&new_action.sa_mask);

	sigaction(SIGINT,&new_action, &old_action);
	sigaction(SIGUSR1,&new_action, &old_action);
	atexit(print_statistic_summary);


	if(g_interval > 0)
	{
		init_statistic(&last_statistic);

		struct sigaction timer_action, old_timer_action ; 
		timer_action.sa_handler = print_realtime_stat ;
		timer_action.sa_flags = 0;
		sigemptyset(&new_action.sa_mask);

		sigaction(SIGALRM, &timer_action, &old_timer_action);

		struct itimerval tick ; 
		memset(&tick, 0, sizeof(tick));

		tick.it_value.tv_sec = g_interval ;
		tick.it_value.tv_usec = 0 ;
		tick.it_interval.tv_sec = g_interval;
		tick.it_interval.tv_usec = 0 ;

		ret = setitimer(ITIMER_REAL, &tick, NULL);
		if(ret)
		{
			fprintf(stderr, "failed to set timer for realtime ops, ignore this error and continue\n");
		}
	}

	for(i = 0 ; i < thread_num ; i++)
	{
		pthread_join(tid_array[i], NULL);
	}

	return 0;
}

