#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>


pthread_mutex_t r_mutex =  PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t w_mutex =  PTHREAD_MUTEX_INITIALIZER;

#define num_blocks  64
#define thread_num  6
#define sortqueue_len  2500000	//~32MB, ~4M numbers

typedef struct _sort_window {
	pthread_mutex_t mutex;
	pthread_cond_t cond_r;
	pthread_cond_t cond_w;
	unsigned long long s_queue[sortqueue_len+1]; //circular queue
	int head; 	
	int tail;
} sort_window;

typedef struct _f_buf_queue {
	unsigned long long s_queue[sortqueue_len+1]; //circular queue
	int head; 	
	int tail;
} f_buf_queue;

typedef struct _inout_window {
	sort_window * p_in_window1;
	sort_window * p_in_window2;
	sort_window * p_out_window;
	int in_fd1;
	int in_fd2;
	int out_fd;
} inout_window;
/*bottom layer read from all sorted files, output to window; middle layers read from and compare and merge to windows;
top layer write every 10 number to final output file.*/

const unsigned long long total_size = 64000000000UL;
const int output_num = 400000000;//final output numbers, interval 10 for each other
unsigned int block_size;	//must be able to be put into memory
unsigned int num_per_block;
const char output_fname[] = "final_sort_nums";

int r_fd;
int w_fds[num_blocks];
char is_end = 0;
char is_end2 = 0;
int curr_block = 0;

unsigned long long * pnum[thread_num];
unsigned long long * sorted_num[thread_num];

int read_nums(unsigned long long * p_nums, int readfd, int blocksize)
{
	int total = 0;
	char * pstart = (char *)p_nums;
	while (total < blocksize)
	{
		int num_once = read(readfd, pstart, blocksize-total);
		if (num_once == -1 )	return -1;
		if (num_once == 0)	break;
		pstart += num_once;
		total += num_once;
	}
	
	return (total/sizeof(unsigned long long));	
}

int write_nums(unsigned long long * p_nums, int w_fd, int blocksize)
{
	int total = 0;
	char * pstart = (char *)p_nums;
	while (total < blocksize)
	{
		int num_once = write(w_fd, pstart, blocksize-total);
		if (num_once == -1 )	return -1;
		//if (num_once == 0)	break;
		pstart += num_once;
		total += num_once;
	}
	return (total/sizeof(unsigned long long));
}

int write_nums2(char * p_nums, int w_fd, int blocksize)
{
	int total = 0;
	char * pstart = (char *)p_nums;
	while (total < blocksize)
	{
		int num_once = write(w_fd, pstart, blocksize-total);
		if (num_once == -1 )	return -1;
		//if (num_once == 0)	break;
		pstart += num_once;
		total += num_once;
	}
	return (total);
}

void _merge_sort(unsigned long long * from, int left, int mid, int end, unsigned long long * sorted)
{
	int i0=left;
	int i1=mid;
	
	for (int j = left; j < end; j++)
	{
		if (i0 < mid && i1 < end)
		{
			if (from[i0] <= from[i1])
			{
				sorted[j] = from[i0];
				i0++;
			}
			else
			{
				sorted[j] = from[i1];
				i1++;
			}
			
		}
		else
		{
			if (i0 >= mid && i1 >= end)
			{
				break;
			}			
			else if (i1 >= end)
			{
				sorted[j] = from[i0];
				i0++;
			}
			else
			{
				sorted[j] = from[i1];
				i1++;
			}
			
		}
	}//end for
	
}

void merge_sort(unsigned long long * from, unsigned long long * sorted, int len)
{
	
	for (int w = 1; w < len; w *= 2)
	{
			for (int i = 0; i < len; i = i+2*w)
			{
				int mid, right;
				mid = (len<=(i+w))?len:(i+w);
				right = (len<=(i+2*w))?len:(i+2*w);
				_merge_sort(from, i, mid, right, sorted);
			}
			unsigned long long * temp = from;
			from = sorted;
			sorted = temp;
	}
			
}

void * sort_block(void * parg)	//thread function
{
	
	unsigned int id = (unsigned long long)parg;
	int w_block = 0;
	while (1)
	{
	char w_fname[256];
	
	pthread_mutex_lock(&r_mutex);
	
	if ((curr_block >= num_blocks) || (is_end))	{pthread_mutex_unlock(&r_mutex); return (void *)0;}
	
	int r_num = read_nums(pnum[id], r_fd, block_size);
	sprintf(w_fname, "sort_group_%d",curr_block);
	w_block = curr_block;
	w_fds[w_block] = -1;
	curr_block++;
	if (r_num != num_per_block)	{is_end = 1;}	
	if (r_num <= 0)	{pthread_mutex_unlock(&r_mutex); return (void *)1;}
		
	pthread_mutex_unlock(&r_mutex);
		
	
	merge_sort(pnum[id], sorted_num[id], r_num);
	
	
	pthread_mutex_lock(&w_mutex);
	int w_fd = open(w_fname, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
	if (w_fd == -1) {perror("Open file for write failed:"); pthread_mutex_unlock(&w_mutex); continue;}
	int w_num = write_nums(sorted_num[id], w_fd, block_size);
	if (w_num!=r_num)	{perror("Write not totally successful:");}
	//close(w_fd);
	w_fds[w_block] = w_fd;
	printf("Block %d/%d complete.\n", w_block+1, num_blocks);
	pthread_mutex_unlock(&w_mutex);
	
	}
	
	return 0;	
}

void * merge_block(void * parg)
{
	inout_window *p_iow = (inout_window *)parg;
	
	f_buf_queue *fbq1;
	f_buf_queue *fbq2;
	char * fwq = NULL;
	int w_num = 0;
	
	if (p_iow->p_in_window1 == NULL && p_iow->p_in_window2 == NULL)	//read from input file
	{
		fbq1 = malloc(sizeof(f_buf_queue));
		if (!fbq1)	{fprintf(stderr, "Warning: malloc alloction failed for f_buf.\n");}
		fbq2 = malloc(sizeof(f_buf_queue));
		if (!fbq2)	{fprintf(stderr, "Warning: malloc alloction failed for f_buf.\n");}
		fbq1->tail=0;
		fbq1->head=0;
		fbq2->tail=0;
		fbq2->head=0;
	}
	
	if (p_iow->out_fd != -1)
	{
		//fwq = (unsigned long long *)malloc(sortqueue_len*sizeof(unsigned long long)/10);
		fwq = (char *)malloc(sortqueue_len/10*20);
		if (!fwq)	{fprintf(stderr, "Warning: malloc alloction failed for output_buf.\n");}
	}
	
	while (1)
	{
	if (p_iow->p_in_window1 == NULL && p_iow->p_in_window2 == NULL)	//use file as input
	{
	
		pthread_mutex_lock(&(p_iow->p_out_window->mutex));
		while((p_iow->p_out_window->tail+1) % (sortqueue_len+1) == p_iow->p_out_window->head)	//full
			pthread_cond_wait(&(p_iow->p_out_window->cond_w), &(p_iow->p_out_window->mutex));
		
		while ((p_iow->p_out_window->tail+1) % (sortqueue_len+1) != p_iow->p_out_window->head)
		{
			if (fbq1->head == fbq1->tail) //file queue empty
			{
				int r_num = read_nums(fbq1->s_queue, p_iow->in_fd1, sortqueue_len*sizeof(unsigned long long));
				if (r_num <= 0) {memset(fbq1->s_queue, 0xff, sizeof(fbq1->s_queue));}//file read to end, fill with maximum number
				fbq1->head = 0;
				fbq1->tail = sortqueue_len;
			}
			if (fbq2->head == fbq2->tail)//file queue empty
			{
				int r_num = read_nums(fbq2->s_queue, p_iow->in_fd2, sortqueue_len*sizeof(unsigned long long));
				if (r_num <= 0) {memset(fbq2->s_queue, 0xff, sizeof(fbq2->s_queue));}//file read to end, fill with maximum number
				fbq2->head = 0;
				fbq2->tail = sortqueue_len;
			}
			
			if (fbq1->s_queue[fbq1->head]<=fbq2->s_queue[fbq2->head])
			{				
				p_iow->p_out_window->s_queue[p_iow->p_out_window->tail] = fbq1->s_queue[fbq1->head];
				p_iow->p_out_window->tail = (p_iow->p_out_window->tail + 1) % (sortqueue_len+1);
				fbq1->head = (fbq1->head + 1) % (sortqueue_len+1);
			}
			else
			{
				p_iow->p_out_window->s_queue[p_iow->p_out_window->tail] = fbq2->s_queue[fbq2->head];
				p_iow->p_out_window->tail = (p_iow->p_out_window->tail + 1) % (sortqueue_len+1);
				fbq2->head = (fbq2->head + 1) % (sortqueue_len+1);
			}				
		}
		
		pthread_cond_signal(&(p_iow->p_out_window->cond_r));
		pthread_mutex_unlock(&(p_iow->p_out_window->mutex));
	}
	else
	{
		pthread_mutex_lock(&(p_iow->p_in_window1->mutex));
		while((p_iow->p_in_window1->tail+1) % (sortqueue_len+1) != p_iow->p_in_window1->head)	//not full
			pthread_cond_wait(&(p_iow->p_in_window1->cond_r), &(p_iow->p_in_window1->mutex));
		
		pthread_mutex_lock(&(p_iow->p_in_window2->mutex));
		while((p_iow->p_in_window2->tail+1) % (sortqueue_len+1) != p_iow->p_in_window2->head)	//not full
			pthread_cond_wait(&(p_iow->p_in_window2->cond_r), &(p_iow->p_in_window2->mutex));
		
		if (p_iow->p_out_window != NULL)
		{
			pthread_mutex_lock(&(p_iow->p_out_window->mutex));
			while ((p_iow->p_out_window->tail+1) % (sortqueue_len+1) == p_iow->p_out_window->head)	//full
				pthread_cond_wait(&(p_iow->p_out_window->cond_w), &(p_iow->p_out_window->mutex));
			
			// because the two buffers (queues) are full before reach here, and we only take out 1 queue's size, buffer will not run empty
			while ((p_iow->p_out_window->tail+1) % (sortqueue_len+1) != p_iow->p_out_window->head)
			{
				if (p_iow->p_in_window1->s_queue[p_iow->p_in_window1->head]<=p_iow->p_in_window2->s_queue[p_iow->p_in_window2->head])
				{				
					p_iow->p_out_window->s_queue[p_iow->p_out_window->tail] = p_iow->p_in_window1->s_queue[p_iow->p_in_window1->head];
					p_iow->p_out_window->tail = (p_iow->p_out_window->tail + 1) % (sortqueue_len+1);
					p_iow->p_in_window1->head = (p_iow->p_in_window1->head + 1) % (sortqueue_len+1);
				}
				else
				{
					p_iow->p_out_window->s_queue[p_iow->p_out_window->tail] = p_iow->p_in_window2->s_queue[p_iow->p_in_window2->head];
					p_iow->p_out_window->tail = (p_iow->p_out_window->tail + 1) % (sortqueue_len+1);
					p_iow->p_in_window2->head = (p_iow->p_in_window2->head + 1) % (sortqueue_len+1);
				}				
			}
			
			pthread_cond_signal(&(p_iow->p_out_window->cond_r));
			pthread_mutex_unlock(&(p_iow->p_out_window->mutex));
		}
		else //final output result
		{
			int index = 0;
			for (int i = 0; i < sortqueue_len; i++)
			{
				if (p_iow->p_in_window1->s_queue[p_iow->p_in_window1->head]<=p_iow->p_in_window2->s_queue[p_iow->p_in_window2->head])
				{	
					if (i % 10 == 0)	
					{
						//fwq[index] = p_iow->p_in_window1->s_queue[p_iow->p_in_window1->head];
						int wn = sprintf(fwq+index, "%llu\n",p_iow->p_in_window1->s_queue[p_iow->p_in_window1->head]);
						index += wn;
					}
					p_iow->p_in_window1->head = (p_iow->p_in_window1->head + 1) % (sortqueue_len+1);
				}
				else
				{
					if (i % 10 == 0)	
					{
						//fwq[index] = p_iow->p_in_window2->s_queue[p_iow->p_in_window2->head];
						int wn = sprintf(fwq+index, "%llu\n",p_iow->p_in_window2->s_queue[p_iow->p_in_window2->head]);
						index += wn;
					}
					p_iow->p_in_window2->head = (p_iow->p_in_window2->head + 1) % (sortqueue_len+1);
				}								
			}
			int nm = write_nums2(fwq, p_iow->out_fd, index);
			if (nm!= index)	{perror("Write not totally successful:");}
			w_num += sortqueue_len/10;
			if (w_num % (output_num/100) == 0)	printf("%d / %d numbers completed.\n", w_num, output_num);
		}
		
		pthread_cond_signal(&(p_iow->p_in_window2->cond_w));
		pthread_mutex_unlock(&(p_iow->p_in_window2->mutex));
		pthread_cond_signal(&(p_iow->p_in_window1->cond_w));
		pthread_mutex_unlock(&(p_iow->p_in_window1->mutex));
		
		if (w_num >= output_num)	{free(fwq);return 0;}
	}
	}// end while;
	return 0;
}
	
int main(int argc, char **argv)
{
	block_size = (unsigned int)(total_size/num_blocks);
	num_per_block = block_size/sizeof(unsigned long long);
	
	if (argc < 2)
	{fprintf(stderr, "Format: mmsort input_file_pathname\n"); return 3;}
	
	r_fd = open(argv[1], O_RDONLY);
	if (r_fd == -1)	{perror("Open file for read failed:"); return 1;}
	
	time_t time_start = time(NULL);
	
	pthread_t pid[thread_num];
	void * res[thread_num];
	int thrd_err;
	
	for (int i = 0; i<thread_num; i++)
	{
		 pnum[i] = (unsigned long long *)malloc(block_size);
		 if (pnum[i] == NULL) {fprintf(stderr,"Error allocating memory.\n"); return 2;}
		 sorted_num[i] = (unsigned long long *)malloc(block_size);
		 if (sorted_num[i] == NULL) {fprintf(stderr,"Error allocating memory.\n"); return 2;}
	}
		
	printf("Now start generating %d sorted block files...\n", num_blocks);
		 
	for (int i = 0; i<thread_num; i++)
	{
		 thrd_err = pthread_create(&pid[i], NULL, &sort_block, (void *)i);
		 if (thrd_err != 0)	perror("Create thread failed:");
		 
	}
	for (int i = 0; i<thread_num; i++)
	{	
		thrd_err = pthread_join(pid[i], &res[i]);
		 if (thrd_err != 0)	perror("Join thread failed:");
		 if ((unsigned long long)(res[i])!= 0)	fprintf(stderr, "Thread return value not 0 but %lld\n", (unsigned long long)(res[i]));
	}
		
	close(r_fd);
	for (int i = 0; i<thread_num; i++)
	{
		free(pnum[i]);
		free(sorted_num[i]);
	}
	
	time_t time_mid = time(NULL);
	int sec = (time_mid-time_start)%60;
	int min = (time_mid-time_start)/60;
	int hour = min/60;
	min = min % 60;
	printf("Time used so far is %d h %d m %d s.\n",hour, min,sec);
	printf("Now start sorting from all %d sorted block files and write sorting result...\n", num_blocks);
	
	int win_num = num_blocks;
	int total_win_num = 0;
	int total_thread_num = 0;
	int win_num_layer[256];
	int thread_num_layer[256];
	int layer = 0;
		
	while (win_num > 2)
	{
		if (win_num % 2 != 0)	win_num = win_num/2+1;
		else	win_num = win_num/2;
		win_num_layer[layer] = win_num;
		thread_num_layer[layer] = win_num;
		total_thread_num += win_num;
		total_win_num += win_num;
		layer++;
	}
	
	thread_num_layer[layer] = 1;
	total_thread_num++;
	
	printf("Creating %d windows for sorting from block files...\n", total_win_num);
	
	sort_window * p_sws = (sort_window*)malloc(sizeof(sort_window)*total_win_num);
	if (p_sws == NULL)	{fprintf(stderr,"Error allocating memory.\n"); return 4;}
		
	inout_window *p_iow = (inout_window*)malloc(sizeof(inout_window)*total_thread_num);
	if (p_iow == NULL)	{fprintf(stderr,"Error allocating memory.\n"); return 4;}
	
	pthread_t * p_pid2 = (pthread_t*)malloc(sizeof(pthread_t)*total_thread_num);
	if (p_pid2 == NULL)	{fprintf(stderr,"Error allocating memory.\n"); return 4;}
	
	for (int i = 0; i<total_win_num; i++)
	{
		pthread_mutex_init(&(p_sws[i].mutex), NULL);
		pthread_cond_init(&(p_sws[i].cond_r), NULL);
		pthread_cond_init(&(p_sws[i].cond_w), NULL);
		p_sws[i].head = 0;
		p_sws[i].tail = 0;
	}
	
	int output_fd = open(output_fname, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
	if (output_fd == -1)	{perror("Open file for write result failed:"); return 5;}
	
	for (int i = 0; i < num_blocks; i++)	//put file pointer at start of input file
	{
		lseek(w_fds[i], 0L, SEEK_SET);
	}
	
	
	int thrd_err2, tnum1=0, twin1=0, old_twin1=0, fd_index=0;	
	
	for (int i = 0; i <= layer; i++)
	{
		int offset = 0;
		for (int j = 0; j < thread_num_layer[i]; j++)
		{
			
			if (i==0)
			{
				p_iow[tnum1+j].p_in_window1 = NULL;
				p_iow[tnum1+j].p_in_window2 = NULL;
				p_iow[tnum1+j].p_out_window = p_sws+twin1+j;
				p_iow[tnum1+j].in_fd1 = w_fds[fd_index];
				fd_index++;
				p_iow[tnum1+j].in_fd2 = w_fds[fd_index];
				fd_index++;
				p_iow[tnum1+j].out_fd = -1;
			}
			else if (i < layer)
			{
				p_iow[tnum1+j].p_in_window1 = p_sws+old_twin1+offset;
				offset++;
				p_iow[tnum1+j].p_in_window2 = p_sws+old_twin1+offset;
				offset++;
				p_iow[tnum1+j].p_out_window = p_sws+twin1+j;
				p_iow[tnum1+j].in_fd1 = -1;
				p_iow[tnum1+j].in_fd2 = -1;
				p_iow[tnum1+j].out_fd = -1;
			}
			else	//i==layer
			{
				p_iow[tnum1+j].p_in_window1 = p_sws+old_twin1+offset;
				offset++;
				p_iow[tnum1+j].p_in_window2 = p_sws+old_twin1+offset;
				offset++;
				p_iow[tnum1+j].p_out_window = NULL;
				p_iow[tnum1+j].in_fd1 = -1;
				p_iow[tnum1+j].in_fd2 = -1;
				p_iow[tnum1+j].out_fd = output_fd;
			}
			
			
		}
		
		tnum1 += thread_num_layer[i];
		if (i < layer)
		{
			old_twin1 = twin1;
			twin1 += win_num_layer[i];
		}
	}
	
	//printf("total_win_num is %d %d total_thread_num is %d %d\n", total_win_num, twin1, total_thread_num, tnum1);
	
	for (int i = 0; i<total_thread_num; i++)
	{	
		thrd_err2 = pthread_create(&p_pid2[i], NULL, &merge_block, &p_iow[i]);
		if (thrd_err2 != 0)	fprintf(stderr,"Create thread %d failed:", i);			
	}
	
	void * res2;
	thrd_err2 = pthread_join(p_pid2[total_thread_num-1], &res2);
	if (thrd_err2 != 0)	perror("Join thread failed:");
	if ((unsigned long long)res2!= 0)	fprintf(stderr, "Thread return value not 0 but %lld\n", (unsigned long long)res2);
		
	for (int i = 0; i<total_thread_num-1; i++)
	{			
		thrd_err2 = pthread_cancel(p_pid2[i]);
		if (thrd_err2 != 0)	perror("Cancel thread failed:");		 
	}

	for (int i = 0;i < num_blocks; i++)
		close(w_fds[i]);
		
	close(output_fd);
	
	time_t time_end = time(NULL);
	sec = (time_end-time_start)%60;
	min = (time_end-time_start)/60;
	hour = min/60;
	min = min % 60;
	printf("Time used so far is %d h %d m %d s.\n",hour, min,sec);
	
	return 0;
}
