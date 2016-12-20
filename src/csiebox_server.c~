#include "csiebox_server.h"

#include "csiebox_common.h"
#include "connect.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdlib.h>

#include<sys/select.h>
#include<dirent.h>
#include<linux/fcntl.h>
#include<hash.h>
#include<pthread.h>

static int parse_arg(csiebox_server* server, int argc, char** argv);
void* handle_request( void* argp );
static int get_account_info(
		csiebox_server* server,  const char* user, csiebox_account_info* info);
static void login(
		csiebox_server* server, int conn_fd, csiebox_protocol_login* login);
static void logout(csiebox_server* server, int conn_fd);
static char* get_user_homedir(
		csiebox_server* server, csiebox_client_info* info);

#define DIR_S_FLAG (S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)//permission you can use to create new file
#define REG_S_FLAG (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)//permission you can use to create new directory

//handle request
static int sync_meta( csiebox_server* server, int conn_fd, csiebox_protocol_meta* meta );
static int sync_file( csiebox_server* server, int conn_fd, char* dirname ,char* filename, struct stat* stat );
static void sync_hardlink( csiebox_server* server, int conn_fd, csiebox_protocol_hardlink* hardlink );
static void rm_file( csiebox_server* server, int conn_fd, csiebox_protocol_rm* rm );
static void handle_login( csiebox_server* server, int conn_fd );

//upload
static int d_rm_file( csiebox_server* server, int conn_fd, char* name );
static int d_hardlink( csiebox_server* server, int conn_fd, char* tgt, char* src );
static int d_sync_file( csiebox_server* server, int conn_fd, char* buffer, int size );
static int d_sync_meta(csiebox_server* server, int conn_fd, char* filepath, struct stat* stat );
void tree_walk( csiebox_server* server, int conn_fd, char* clientdir ,char* dirpath, struct hash* inodemap );


typedef struct{
	csiebox_server* server;
	int conn_fd;
	fd_set* master;
}arg;

typedef struct {
	int threadnum ;
	pthread_t* threads ;
	int count ;
	arg* argp ;
}threadpool;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER ;
threadpool* t_pool = NULL ;

//read config file, and start to listen
void csiebox_server_init(
		csiebox_server** server, int argc, char** argv) {
	csiebox_server* tmp = (csiebox_server*)malloc(sizeof(csiebox_server));
	if (!tmp) {
		fprintf(stderr, "server malloc fail\n");
		return;
	}
	memset(tmp, 0, sizeof(csiebox_server));
	if (!parse_arg(tmp, argc, argv)) {
		fprintf(stderr, "Usage: %s [config file]\n", argv[0]);
		free(tmp);
		return;
	}
	int fd = server_start();
	if (fd < 0) {
		fprintf(stderr, "server fail\n");
		free(tmp);
		return;
	}
	tmp->client = (csiebox_client_info**)
		malloc(sizeof(csiebox_client_info*) * getdtablesize());
	if (!tmp->client) {
		fprintf(stderr, "client list malloc fail\n");
		close(fd);
		free(tmp);
		return;
	}
	memset(tmp->client, 0, sizeof(csiebox_client_info*) * getdtablesize());
	tmp->listen_fd = fd;
	*server = tmp;
	
}

//wait client to connect and handle requests from connected socket fd
//===============================
//		TODO: you need to modify code in here and handle_request() to support I/O multiplexing
//===============================
int csiebox_server_run(csiebox_server* server ) {
	int conn_fd, conn_len;
	struct sockaddr_in addr;
	int fdmax ;
	fd_set master ;
	fd_set read_fds ;
	
	FD_ZERO(&master) ;
	FD_ZERO(&read_fds) ;
	
	FD_SET(server->listen_fd,&master) ;
	fdmax = server->listen_fd ;
	
	//initialize thread pool
	t_pool = (threadpool*)malloc(sizeof(threadpool)) ;
	memset(t_pool,0,sizeof(t_pool));
	t_pool->threadnum = (server)->arg.thread_num ;
	t_pool->threads = (pthread_t*)malloc(sizeof(pthread_t)*t_pool->threadnum) ;
	t_pool->count = 0 ;
	t_pool->argp = (arg*)malloc(sizeof(arg));
	memset( t_pool->argp, 0, sizeof(arg) );
	
	t_pool->argp->server = server ;
	t_pool->argp->master = &master ;
	for(int i = 0 ; i < t_pool->threadnum ; i++)
		pthread_create( &t_pool->threads[i], NULL, handle_request, t_pool->argp ) ;
	
	//fprintf(stderr,"%d\n",t_pool->argp[1].conn_fd);
	

	//fprintf(stderr,"%d\n",server->arg.thread_num);
	
	while (1) {
		memset(&addr, 0, sizeof(addr));
		conn_len = 0 ;
		// waiting client connect
		read_fds = master ;
		
		struct timeval time;
		memset(&time,0,sizeof(time));

		time.tv_sec = 3 ;
		
		if( select(fdmax+1,&read_fds,NULL,NULL,&time  ) == -1 ){
			perror("select") ;
			exit(4) ;
		}
		
		for( int i = 0 ; i <= fdmax ; i++ ){
			if( FD_ISSET(i,&read_fds) ){
				if( i == server->listen_fd ){
					conn_fd = accept(
							server->listen_fd, (struct sockaddr*)&addr, (socklen_t*)&conn_len);
					
					if (conn_fd < 0) {
						if (errno == ENFILE) {
							fprintf(stderr, "out of file descriptor table\n");
							continue;
						} else if (errno == EAGAIN || errno == EINTR) {
							continue;
						} else {
							fprintf(stderr, "accept err\n");
							fprintf(stderr, "code: %s\n", strerror(errno));
							break;
						}
					}
					FD_SET(conn_fd,&master) ;
					if( conn_fd > fdmax ){
						fdmax = conn_fd ;
					}
					//handle client login
					handle_login( server, conn_fd );
				}
				else{
					// handle request from connected socket fd
					
					csiebox_protocol_header check_req ;
					memset(&check_req,0,sizeof(check_req));
					
					//fprintf(stderr,"%d\n",i);

					if( recv_message(i,&check_req,sizeof(check_req)) ){
						
						csiebox_protocol_header check_res ;
						memset(&check_res,0,sizeof(check_res));
						check_res.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
						check_res.res.op = 0x78;
						
						
						//no thread available
						if(t_pool->count == t_pool->threadnum){
							check_res.res.status = 0x04 ;
						}
						if(t_pool->count < t_pool->threadnum){
							check_res.res.status = 0x05 ;

							pthread_mutex_lock(&mutex);
							
							t_pool->argp->conn_fd = i ;
							
							t_pool->count++;
							
							pthread_mutex_unlock(&mutex);
							
							FD_CLR(i, &master) ;
							
							pthread_cond_signal(&cond);
						}
						send_message(i,&check_res,sizeof(check_res));
					}
					else{
						fprintf(stderr, "end of connection\n");
						FD_CLR(i, &master) ;
						logout(server, i);	
					}
				}
			}	
		}

	}
	return 1;
}

void csiebox_server_destroy(csiebox_server** server) {
	csiebox_server* tmp = *server;
	*server = 0;
	if (!tmp) {
		return;
	}
	close(tmp->listen_fd);
	int i = getdtablesize() - 1;
	for (; i >= 0; --i) {
		if (tmp->client[i]) {
			free(tmp->client[i]);
		}
	}
	free(tmp->client);
	free(tmp);
}

//read config file
static int parse_arg(csiebox_server* server, int argc, char** argv) {
	if (argc != 2) {
		return 0;
	}
	FILE* file = fopen(argv[1], "r");
	if (!file) {
		return 0;
	}
	fprintf(stderr, "reading config...\n");
	size_t keysize = 20, valsize = 20;
	char* key = (char*)malloc(sizeof(char) * keysize);
	char* val = (char*)malloc(sizeof(char) * valsize);
	ssize_t keylen, vallen;
	int accept_config_total = 3;
	int accept_config[3] = {0, 0, 0};
	while ((keylen = getdelim(&key, &keysize, '=', file) - 1) > 0) {
		key[keylen] = '\0';
		vallen = getline(&val, &valsize, file) - 1;
		val[vallen] = '\0';
		fprintf(stderr, "config (%d, %s)=(%d, %s)\n", keylen, key, vallen, val);
		if (strcmp("path", key) == 0) {
			if (vallen <= sizeof(server->arg.path)) {
				strncpy(server->arg.path, val, vallen);
				accept_config[0] = 1;
			}
		} else if (strcmp("account_path", key) == 0) {
			if (vallen <= sizeof(server->arg.account_path)) {
				strncpy(server->arg.account_path, val, vallen);
				accept_config[1] = 1;
			}
		}
		else if( strcmp("thread_num",key) == 0 ){
			server->arg.thread_num = (int)strtol(val,(char**)NULL, 10);
			accept_config[2] = 1 ;
		}
	}
	free(key);
	free(val);
	fclose(file);
	int i, test = 1;
	for (i = 0; i < accept_config_total; ++i) {
		test = test & accept_config[i];
	}
	if (!test) {
		fprintf(stderr, "config error\n");
		return 0;
	}
	return 1;
}

static void handle_login( csiebox_server* server, int conn_fd ){
	csiebox_protocol_header header;
	memset(&header,0,sizeof(header));
	if(recv_message(conn_fd,&header,sizeof(header))){
		if(header.req.magic == CSIEBOX_PROTOCOL_MAGIC_REQ){
			if(header.req.op == CSIEBOX_PROTOCOL_OP_LOGIN){
				fprintf(stderr, "login\n");
				csiebox_protocol_login req;
				if (complete_message_with_header(conn_fd, &header, &req)) {
					login(server, conn_fd, &req);			
				}
			}
		}
	}
}



//this is where the server handle requests, you should write your code here
void* handle_request( void* argp ) {
	while(1){
		//lock mutex
		pthread_mutex_lock(&mutex);

		//wait cond
		pthread_cond_wait(&cond,&mutex);
		int conn_fd = ((arg*)argp)->conn_fd;
		//fprintf(stderr,"%d\n",conn_fd) ;

		//unlock mutex
		pthread_mutex_unlock(&mutex);

		csiebox_protocol_header header;
		memset(&header, 0, sizeof(header));
		if( recv_message(conn_fd, &header, sizeof(header)) ) {
			//fprintf(stderr,"here\n");
			if (header.req.magic == CSIEBOX_PROTOCOL_MAGIC_REQ) { 
				switch (header.req.op) {
					/*case CSIEBOX_PROTOCOL_OP_LOGIN:
					  fprintf(stderr, "login\n");
					  csiebox_protocol_login req;
					  if (complete_message_with_header(conn_fd, &header, &req)) {
					  login(server, conn_fd, &req);
					  char* clientdir = get_user_homedir(server, server->client[conn_fd]);

					  DIR* dirp = opendir(clientdir);
					  struct dirent* content ;
					  int num = 0 ;
					//check client dir
					while( (content = readdir( dirp )) != NULL ){
					if( content->d_name[0] !='.' )
					num++;
					if( num > 2 )
					break ;
					}
					closedir(dirp);
					if(num > 2){
					struct hash inodemap ;
					memset( &inodemap, 0, sizeof(inodemap) );
					if( !init_hash(&inodemap,10000) )
					fprintf(stderr,"init hash fail\n");

					tree_walk( server, conn_fd, clientdir ,NULL, &inodemap );
					csiebox_protocol_header stop ;
					memset(&stop,0,sizeof(stop));
					stop.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ;
					stop.req.op = 0x87 ;
					send_message(conn_fd,&stop,sizeof(stop));
					}

					}
					break;*/
					case CSIEBOX_PROTOCOL_OP_SYNC_META:
						fprintf(stderr, "sync meta\n");
						csiebox_protocol_meta meta;
						if (complete_message_with_header(conn_fd, &header, &meta)) {

							if( !sync_meta( ((arg*)argp)->server, conn_fd, &meta ) ){
								fprintf(stderr,"sync meta error\n") ;
							}
						}
						break;
					case CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK:
						fprintf(stderr, "sync hardlink\n");
						csiebox_protocol_hardlink hardlink;
						if (complete_message_with_header(conn_fd, &header, &hardlink)) {
							sync_hardlink( ((arg*)argp)->server, conn_fd,&hardlink );
						}
						break;
					case CSIEBOX_PROTOCOL_OP_SYNC_END:
						fprintf(stderr, "sync end\n");
						csiebox_protocol_header end;
						//====================
						//        TODO: here is where you handle end of synchronization request from client
						//====================
						break;
					case CSIEBOX_PROTOCOL_OP_RM:
						fprintf(stderr, "rm\n");
						csiebox_protocol_rm rm;
						if (complete_message_with_header(conn_fd, &header, &rm)) {
							rm_file( ((arg*)argp)->server, conn_fd, &rm ) ;
						}
						break;
					default:
						fprintf(stderr, "unknown op %x\n", header.req.op);
						break;
				}
				//reset master
				FD_SET(conn_fd,((arg*)argp)->master);
			}
		}

		//mod count
		pthread_mutex_lock(&mutex);
		t_pool->count-- ;
		pthread_mutex_unlock(&mutex);

	}
}

//open account file to get account information
static int get_account_info(
		csiebox_server* server,  const char* user, csiebox_account_info* info) {
	FILE* file = fopen(server->arg.account_path, "r");
	if (!file) {
		return 0;
	}
	size_t buflen = 100;
	char* buf = (char*)malloc(sizeof(char) * buflen);
	memset(buf, 0, buflen);
	ssize_t len;
	int ret = 0;
	int line = 0;
	while ((len = getline(&buf, &buflen, file) - 1) > 0) {
		++line;
		buf[len] = '\0';
		char* u = strtok(buf, ",");
		if (!u) {
			fprintf(stderr, "illegal form in account file, line %d\n", line);
			continue;
		}
		if (strcmp(user, u) == 0) {
			memcpy(info->user, user, strlen(user));
			char* passwd = strtok(NULL, ",");
			if (!passwd) {
				fprintf(stderr, "illegal form in account file, line %d\n", line);
				continue;
			}
			md5(passwd, strlen(passwd), info->passwd_hash);
			ret = 1;
			break;
		}
	}
	free(buf);
	fclose(file);
	return ret;
}

//handle the login request from client
static void login(
		csiebox_server* server, int conn_fd, csiebox_protocol_login* login) {
	int succ = 1;
	csiebox_client_info* info =
		(csiebox_client_info*)malloc(sizeof(csiebox_client_info));
	memset(info, 0, sizeof(csiebox_client_info));
	if (!get_account_info(server, login->message.body.user, &(info->account))) {
		fprintf(stderr, "cannot find account\n");
		succ = 0;
	}
	if (succ &&
			memcmp(login->message.body.passwd_hash,
				info->account.passwd_hash,
				MD5_DIGEST_LENGTH) != 0) {
		fprintf(stderr, "passwd miss match\n");
		succ = 0;
	}

	csiebox_protocol_header header;
	memset(&header, 0, sizeof(header));
	header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES;
	header.res.op = CSIEBOX_PROTOCOL_OP_LOGIN;
	header.res.datalen = 0;
	if (succ) {
		if (server->client[conn_fd]) {
			free(server->client[conn_fd]);
		}
		info->conn_fd = conn_fd;
		server->client[conn_fd] = info;
		header.res.status = CSIEBOX_PROTOCOL_STATUS_OK;
		header.res.client_id = info->conn_fd;
		char* homedir = get_user_homedir(server, info);
		mkdir(homedir, DIR_S_FLAG);
		free(homedir);
	} else {
		header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL;
		free(info);
	}
	send_message(conn_fd, &header, sizeof(header));
}

static void logout(csiebox_server* server, int conn_fd) {
	free(server->client[conn_fd]);
	server->client[conn_fd] = 0;
	close(conn_fd);
}

static char* get_user_homedir(
		csiebox_server* server, csiebox_client_info* info) {
	char* ret = (char*)malloc(sizeof(char) * PATH_MAX);
	memset(ret, 0, PATH_MAX);
	sprintf(ret, "%s/%s", server->arg.path, info->account.user);
	return ret;
}

static int sync_meta( csiebox_server* server, int conn_fd, csiebox_protocol_meta* meta ){

	char buf[1024] = {0} ;
	//recv file path

	if( recv_message( conn_fd, buf, sizeof(buf) ) ){
		char filename[1024] = {0} ;
		int exist = 1 ;
		int mod = 0 ;

		memcpy( filename, buf, meta->message.body.pathlen ) ;

		char dirname[1024] = {0};
		strcat( dirname, get_user_homedir( server, server->client[conn_fd] ) ) ;
		strcat(dirname,"/") ;
		char fullname[1024] ;
		memset(fullname,0,sizeof(fullname)) ;
		strcat( fullname, dirname ) ;
		strcat( fullname, filename ) ;

		struct stat stat ;
		if( lstat( fullname, &stat ) != 0 ){
			//no such  file
			exist = 0 ;
			fprintf(stderr,"stat fail || no such file\n") ;
		}

		int block = 0 ;

		if( exist ){
			//check lock

			struct flock lock ;
			memset(&lock,0,sizeof(lock));
			lock.l_type = F_WRLCK;
			lock.l_whence = SEEK_SET ;
			lock.l_start = 0 ;
			lock.l_len = 0 ;
			lock.l_pid = getpid();
			int fd = open(fullname,O_WRONLY);
			if( fcntl(fd,F_GETLK,&lock) == -1 ){
				fprintf(stderr,"fail to check lock\n");
			}
			close(fd);
			if( lock.l_type == F_UNLCK ){
				block = 0 ;
				//reply
				send_message(conn_fd,&block, sizeof(block));
			}
			//block
			if( lock.l_type == F_WRLCK ){
				block = 1 ;
				//reply
				send_message(conn_fd,&block, sizeof(block));
				return 2 ;
			}

			//apply exclusive lock
			struct flock elock ;
			memset(&elock,0,sizeof(elock));
			elock.l_type = F_WRLCK;
			elock.l_whence = SEEK_SET;
			elock.l_start = 0 ;
			elock.l_len = 0 ;
			elock.l_pid = getpid();
			fd = open(fullname,O_WRONLY);
			if( fcntl(fd,F_SETLK,&elock) == -1 ){
				fprintf(stderr,"fail to set exclusive lock\n");
			}

			uint8_t md5[MD5_DIGEST_LENGTH] ;
			if( S_ISLNK(meta->message.body.stat.st_mode)||S_ISREG(meta->message.body.stat.st_mode) ){
				md5_file( fullname, md5 ) ;

				if( memcmp(md5,meta->message.body.hash,MD5_DIGEST_LENGTH) != 0 || !exist ){
					mod = 1 ;
					fprintf(stderr,"hash change\n");
				}

				if( !S_ISLNK(meta->message.body.stat.st_mode) )
					if( chmod(fullname, meta->message.body.stat.st_mode ) != 0 )
						fprintf(stderr,"fail to chmod\n") ;
			}
			if(!mod){	
				DIR* dirp = opendir(dirname) ;
				struct timespec time[2] ;
				time[0].tv_sec = meta->message.body.stat.st_atim.tv_sec ;
				time[0].tv_nsec = meta->message.body.stat.st_atim.tv_nsec ;
				time[1].tv_sec = meta->message.body.stat.st_mtim.tv_sec ;
				time[1].tv_nsec = meta->message.body.stat.st_mtim.tv_nsec ;	

				if( utimensat( dirfd(dirp), filename, time,AT_SYMLINK_NOFOLLOW) == -1  ){
					fprintf(stderr,"sync meta time fail\n");
					return 0 ;
				}
				closedir(dirp);
				//release lock
				elock.l_type = F_UNLCK ;
				
				if( fcntl(fd,F_SETLK,&elock) == -1 ){
					fprintf(stderr,"fail to unlock\n");
				}
				close(fd);
			}
		}

		csiebox_protocol_header header ;
		memset(&header,0,sizeof(header)) ;

		if( mod || !exist ){
			if(!exist){
				block = 0 ;
				send_message(conn_fd,&block,sizeof(block));
			}

			header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
			header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
			if( S_ISDIR(meta->message.body.stat.st_mode) )
				header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
			else	
				header.res.status = CSIEBOX_PROTOCOL_STATUS_MORE ;

			send_message( conn_fd, &header, sizeof(header) ) ;

			if( !sync_file(server, conn_fd, dirname ,filename , &meta->message.body.stat ) ){
				fprintf(stderr,"sync file fail\n") ;
				return 0 ;
			}

			return 1;
		}
		else{
			header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
			header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
			header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
			send_message( conn_fd, &header, sizeof(header) ) ;
			return 1;
		}
	}
	return 0;
}

static int sync_file( csiebox_server* server, int conn_fd, char* dirname, char* filename, struct stat* stat ){
	csiebox_protocol_file file ;
	memset(&file,0,sizeof(file)) ;

	char fullname[1024] ={0};
	strcat(fullname,dirname);
	strcat(fullname,filename);

	struct flock elock ;
	memset(&elock,0,sizeof(elock));
	elock.l_type = F_WRLCK;
	elock.l_whence = SEEK_SET;
	elock.l_start = 0 ;
	elock.l_len = 0 ;
	elock.l_pid = getpid();
	FILE* fp ;

	if(S_ISDIR(stat->st_mode)){
		mkdir(fullname,DIR_S_FLAG);
	} 

	//recv total datalen
	else if( recv_message( conn_fd, &file, sizeof(file) ) ){
		if(file.message.header.req.magic == CSIEBOX_PROTOCOL_MAGIC_REQ &&
				file.message.header.req.op == CSIEBOX_PROTOCOL_OP_SYNC_FILE
		  ){	
			if( S_ISREG(stat->st_mode) ){
				//fprintf(stderr,"here\n");
				//fprintf(stderr,filename) ;
				/*for(int i =0;i<strlen(filename);i++ ){
				  fprintf(stderr,"%c",filename[i]);
				  fprintf(stderr,"\n");
				  }*/
				char buf[1024] ;
				memset(buf,0,sizeof(buf));
				int size = (int)stat->st_size ;

				if( !(fp=fopen( fullname, "w+" )) ){
					fprintf(stderr,"fail to open file\n");
				}
				//set exclusive lock
				if( fcntl(fileno(fp),F_SETLK,&elock) == -1 ){
					fprintf(stderr,"fail to set exclusive lock\n");
				}

				//fprintf(stderr,filename) ;
				/*fprintf(stderr,"\n");
				  getcwd(buf,PATH_MAX) ;
				  fprintf(stderr,buf) ;*/
				for( int i = size ; i >0; i-= sizeof(buf)  ){
					int rbyte = 0 ;
					if(i < 1024){	
						recv_message( conn_fd, buf, i );
						rbyte = i ;
					}
					else{
						recv_message(conn_fd, buf, sizeof(buf));
						rbyte = sizeof(buf) ;
					}
					fwrite( buf, sizeof(char), rbyte, fp ) ;
				}	
			}
			if( S_ISLNK(stat->st_mode) ){
				//content of link should < PATH_MAX
				char tgt[PATH_MAX] = {0} ;
				recv_message( conn_fd, tgt, PATH_MAX );
				unlink( fullname );
				if( symlink(tgt,fullname) != 0 ){
					fprintf(stderr,"creat symlnk fail\n");
				}
			}
		}
	}
	struct timespec time[2] ;
	time[0].tv_sec = stat->st_atim.tv_sec ;
	time[0].tv_nsec = stat->st_atim.tv_nsec ;
	time[1].tv_sec = stat->st_mtim.tv_sec ;
	time[1].tv_nsec = stat->st_mtim.tv_nsec ;	

	DIR* dirp = opendir(dirname);

	if( utimensat( dirfd(dirp), filename, time, AT_SYMLINK_NOFOLLOW) == -1  ){
		fprintf(stderr,"sync meta time fail\n");
		return 0 ;
	}
	if( S_ISREG(stat->st_mode) ){
		//release lock
		elock.l_type = F_UNLCK ;
		if( fcntl(fileno(fp),F_SETLK,&elock) == -1){
			fprintf(stderr,"fail to unlock\n");
		}
		fclose(fp) ;
	}
	return 1 ;
}

static void sync_hardlink( csiebox_server* server, int conn_fd, csiebox_protocol_hardlink* hardlink ){
	char tgt[1024]={0} ;
	char src[1024]={0} ;
	char buf[1024]={0} ;

	char dirname[PATH_MAX] = {0} ;

	strcat(dirname,get_user_homedir( server, server->client[conn_fd] )) ;
	strcat(dirname,"/") ;

	strcat( tgt, dirname ) ;
	strcat( src, dirname ) ;

	//recv target name
	if( recv_message( conn_fd, &buf, hardlink->message.body.targetlen) ){
		strcat( tgt, buf ) ;
	}
	//recv source name
	if( recv_message(conn_fd, &buf, hardlink->message.body.srclen ) ){
		strcat( src, buf ) ;
	}

	/*fprintf(stderr,"tgt and src\n");
	  fprintf(stderr,"%s\n",tgt);
	  fprintf(stderr,"%s\n",src) ;*/

	csiebox_protocol_header header ;
	memset(&header,0,sizeof(header)) ;
	header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
	header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK ;
	header.res.datalen = 0 ;
	if( link( tgt, src)  == 0 ){
		header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
	}
	else{
		header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL ;
		fprintf(stderr,"sync hardlink fail\n");
	}
	send_message( conn_fd, &header, sizeof(header) ) ;

}

static void rm_file( csiebox_server* server, int conn_fd, csiebox_protocol_rm* rm ){
	char fullname[1024]={0} ;
	char buf[1024]={0} ;

	char dirname[1024] = {0};
	strcat(dirname,get_user_homedir( server, server->client[conn_fd] )) ;

	strcat( dirname, "/" ) ;
	strcpy( fullname, dirname );
	//recv name

	if( recv_message( conn_fd, buf, rm->message.body.pathlen ) ){
		strcat( fullname, buf ) ;
	}

	csiebox_protocol_header header ;
	memset( &header, 0, sizeof(header) ) ;
	header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
	header.res.op = CSIEBOX_PROTOCOL_OP_RM ;

	//rm dir
	if( fullname[strlen(fullname)] == '/' ){
		if( rmdir(fullname) == -1 ){
			header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL ;
			fprintf(stderr,"fail to remove dir\n");
		}
	}
	//rm file
	else{
		if( remove(fullname) == -1  ){
			header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL ;
			fprintf(stderr,"fail to remove file\n");
		}
	}
	header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
	//send res
	send_message( conn_fd, &header, sizeof(header) ) ;
}

void tree_walk( csiebox_server* server, int conn_fd, char* clientdir ,char* dirpath, struct hash* inodemap ){
	char dir[1024] = {0};
	strcat(dir,clientdir);
	strcat(dir,"/");
	if(dirpath != NULL)
		strcat(dir,dirpath);

	DIR* dirp = opendir( dir ) ;
	struct dirent* content ;

	while( (content = readdir( dirp  )) != NULL ){
		if(content->d_name[0] != '.' ){
			if( content->d_name[0] != '.' ){

				char* filename = (char*)malloc(PATH_MAX);
				memset(filename,0,sizeof(filename));
				if( dirpath != NULL )
					strcat(filename, dirpath);

				strcat( filename, content->d_name ) ;

				char* fullname = (char*)malloc(PATH_MAX);
				memset(fullname,0,sizeof(fullname));
				strcat(fullname,clientdir);
				strcat(fullname,"/");
				strcat(fullname,filename);

				struct stat stat ;
				memset(&stat,0,sizeof(stat)) ;

				/*fprintf(stderr,"%s\n",filename);
				  fprintf(stderr,"%s\n",fullname) ;
				  */
				if( lstat(fullname, &stat) != 0 ){
					fprintf(stderr,"fail to load download stat\n") ;
				}

				//check hard link
				int hlink = 0 ;
				char* tgt = NULL ;
				//fprintf(stderr,"%d\n",(int)stat.st_nlink);
				if( (int)stat.st_nlink > 1 ){
					if( get_from_hash( inodemap, (void**)&tgt, (int)stat.st_ino ) ){
						hlink = 1 ;
					}
					else{
						put_into_hash( inodemap, (void*)filename, (int)stat.st_ino );
					}
				}
				if( content->d_type == DT_DIR ){
					int metares = d_sync_meta( server,conn_fd,filename, &stat ) ;

					if( !metares ){
						fprintf( stderr, "download sync_meta error\n" ) ;
					}
					strcat(filename,"/") ;
					tree_walk( server, conn_fd, clientdir ,filename, inodemap )  ;

				}
				if( content->d_type == DT_REG ){	
					if( hlink ){
						d_hardlink( server, conn_fd, tgt, filename ) ;
					}
					else{
						int metares = d_sync_meta( server, conn_fd, filename, &stat ) ;	
						if( !metares ){
							fprintf( stderr, "download sync_meta error\n" ) ;
						}
						if(metares == 2){
							FILE* fp = fopen(fullname,"r") ;

							fseek(fp,0,SEEK_SET) ;
							char buf[1024] = {0} ;
							int size = (int)stat.st_size ;
							d_sync_file( server, conn_fd, buf, size ) ;
							for( int i = size ; i > 0 ; i -= 1024 ){
								if( i < 1024){
									fread( buf, sizeof(char), i, fp ) ;
									//fprintf(stderr,buf) ;
									//fprintf(stderr,"\n") ;
									if( !send_message( conn_fd, buf, i ) ){
										fprintf(stderr,"send file content fail\n") ;
									}
								}
								else{
									fread( buf, sizeof(char), 1024, fp ) ;
									if( !send_message( conn_fd, buf, 1024 ) ){
										fprintf(stderr,"send file content fail\n") ;
									}
								}

							}
							fclose(fp);
						}
					}
				}
				if( content->d_type == DT_LNK ){
					if( d_sync_meta( server, conn_fd, filename, &stat ) == 2 ){
						char buf[PATH_MAX] = {0};
						int size = (int)stat.st_size ;

						d_sync_file( server, conn_fd, buf, size ) ;

						readlink( fullname, buf, PATH_MAX ) ;
						if( size < PATH_MAX )
							buf[size]= '\0' ;
						else
							fprintf(stderr,"symlink too long\n");

						if( !send_message( conn_fd, buf, PATH_MAX ) )
							fprintf(stderr,"send symlink content fail\n") ;
					}
				}			
			}
		}
	}
	closedir(dirp);
}
static int d_sync_meta(csiebox_server* server, int conn_fd, char* filepath, struct stat* stat ){
	csiebox_protocol_meta req ;
	memset(&req,0,sizeof(req)) ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
	//req.message.header.req.client_id = server->
	req.message.header.req.datalen = sizeof(req) - sizeof(req.message.header) ;
	req.message.body.pathlen = strlen(filepath) ;

	req.message.body.stat = *stat;
	/*fprintf(stderr,"%d\n",(int)stat->st_size);
	  fprintf(stderr,"%d\n",req.message.body.stat.st_size);*/
	if( S_ISREG(stat->st_mode) )
		md5_file( filepath, req.message.body.hash ) ;
	//send meta
	if( !send_message( conn_fd, &req, sizeof(req) ) ){
		fprintf(stderr,"send fail\n") ;
		return 0 ;
	}

	//send filepath

	if( !send_message( conn_fd, filepath, 1024 ) ){
		fprintf(stderr,"send filepath fail\n") ;
	}
	csiebox_protocol_header header ;
	memset( &header, 0 ,sizeof(header) ) ;
	//recv res
	if( recv_message(conn_fd, &header, sizeof(header) ) ){
		if( header.res.magic == CSIEBOX_PROTOCOL_MAGIC_RES && 
				header.res.op == CSIEBOX_PROTOCOL_OP_SYNC_META ){

			if( header.res.status == CSIEBOX_PROTOCOL_STATUS_OK ){ 
				return 1 ;
			}
			else if( header.res.status == CSIEBOX_PROTOCOL_STATUS_FAIL )
				return 0 ;
			else if( header.res.status == CSIEBOX_PROTOCOL_STATUS_MORE ){
				return 2 ;
			}
		}
	}
	return 0 ;
}
static int d_sync_file( csiebox_server* server, int conn_fd, char* buffer, int size ){	
	csiebox_protocol_file req ;
	memset(&req,0,sizeof(req)) ;
	req.message.body.datalen = size ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_FILE ;
	//req.message.header.req.client_id = client->client_id ;

	if( !send_message( conn_fd, &req, sizeof(req) ) )
		fprintf(stderr,"send datalen fail\n") ;

	return 1 ;
}
static int d_hardlink( csiebox_server* server, int conn_fd, char* tgt, char* src ){
	csiebox_protocol_hardlink req ;
	memset( &req, 0, sizeof(req) ) ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK ;
	//req.message.header.req.client_id = client->client_id ;
	req.message.header.req.datalen = sizeof(req)-sizeof(req.message.header) ;

	/*fprintf(stderr,"tgt and src\n");
	  fprintf(stderr,"%s\n",tgt);
	  fprintf(stderr,"%s\n",src);
	  */
	req.message.body.srclen = strlen(src)+1 ;
	req.message.body.targetlen = strlen(tgt)+1 ;
	//send hardlink req
	if( !send_message( conn_fd, &req, sizeof(req) ) )
		fprintf(stderr,"send hardlink fail\n");

	//send target path
	/*fprintf(stderr,"%d\n",strlen(tgt));
	  fprintf(stderr,"%d\n",strlen(src));
	  */
	if( !send_message( conn_fd, tgt, strlen(tgt)+1 ) ){
		fprintf(stderr,"send target fail\n");
		return 0 ;
	}
	//send source path
	//fprintf(stderr,"%d\n",strlen(src));
	if( !send_message( conn_fd, src, strlen(src)+1 ) ){
		fprintf(stderr,"send target fail\n");
		return 0 ;
	}

	//recv result
	csiebox_protocol_header header ;
	memset( &header, 0, sizeof(header) ) ;
	if( recv_message(conn_fd, &header, sizeof(header)) ) {
		if( header.res.magic == CSIEBOX_PROTOCOL_MAGIC_RES &&
				header.res.op == CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK &&
				header.res.status == CSIEBOX_PROTOCOL_STATUS_OK ){
			return 1 ;
		}
		else
			return 0 ;
	}
	return 0 ;
}
static int d_rm_file( csiebox_server* server, int conn_fd, char* name ){
	csiebox_protocol_rm rm ;
	memset( &rm, 0, sizeof(rm) ) ;
	rm.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	rm.message.header.req.op = CSIEBOX_PROTOCOL_OP_RM ;
	rm.message.header.req.datalen = sizeof(rm)-sizeof(rm.message.header);
	//rm.message.header.req.client_id = client->client_id ;
	rm.message.body.pathlen = strlen(name) + 1 ;

	//fprintf(stderr,"%d\n",rm.message.body.pathlen);

	//send rm req
	if( !send_message( conn_fd, &rm, sizeof(rm) ) ){
		fprintf(stderr,"fail to send rm req\n");
	}
	//send rm file name
	if( !send_message( conn_fd, name, strlen(name)+1 )  ){
		fprintf(stderr,"fail to send rm file name\n");
	}
	csiebox_protocol_header header ;
	memset( &header, 0, sizeof(header) ) ;

	if( recv_message( conn_fd, &header, sizeof(header) ) ){
		if( header.res.status == CSIEBOX_PROTOCOL_STATUS_OK )
			return 1 ;
		else
			return 0 ;
	}
	return 0 ;
}
