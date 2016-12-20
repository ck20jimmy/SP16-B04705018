#include "csiebox_client.h"

#include "csiebox_common.h"
#include "connect.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include<dirent.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<hash.h>
#include<linux/fcntl.h>
#include<sys/inotify.h>


static int parse_arg(csiebox_client* client, int argc, char** argv);
static int login(csiebox_client* client);

//upload
static int sync_meta(csiebox_client* client, char* filepath, struct stat* stat ) ;
static int sync_file( csiebox_client* client, char* filepath, int size ) ;
static int hardlink( csiebox_client* client, char* tgt, char* src );
static int rm_file( csiebox_client* client, char* name ) ;
void tree_walk( csiebox_client* client, char* clientdir ,char* dirpath, struct hash* inodemap, struct hash* inotify, int ifd ); 

//download
#define DIR_S_FLAG (S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)//permission you can use to create new file
#define REG_S_FLAG (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)//permission you can use to create new directory
static void d_rm_file( csiebox_client* client, csiebox_protocol_rm* rm );
static void d_sync_hardlink( csiebox_client* client, csiebox_protocol_hardlink* hardlink, struct hash* inotify, int ifd );
static int d_sync_file( csiebox_client* client, char* dirname, char* filename, struct stat* stat, struct hash* inotify, int ifd );
static int d_sync_meta( csiebox_client* client,  csiebox_protocol_meta* meta, struct hash* inotify, int ifd );
static void download(csiebox_client* client , struct hash* inotify, int ifd );
//inotify
#define EVENT_SIZE (sizeof(struct inotify_event))
#define EVENT_BUF_LEN (1024 * (EVENT_SIZE + 16))
void inotify_file( csiebox_client* client, int ifd, struct hash* inotify, int cwd, int num );


//read config file, and connect to server
void csiebox_client_init(
		csiebox_client** client, int argc, char** argv) {
	csiebox_client* tmp = (csiebox_client*)malloc(sizeof(csiebox_client));
	if (!tmp) {
		fprintf(stderr, "client malloc fail\n");
		return;
	}
	memset(tmp, 0, sizeof(csiebox_client));
	if (!parse_arg(tmp, argc, argv)) {
		fprintf(stderr, "Usage: %s [config file]\n", argv[0]);
		free(tmp);
		return;
	}
	int fd = client_start(tmp->arg.name, tmp->arg.server);

	if (fd < 0) {
		fprintf(stderr, "connect fail\n");
		free(tmp);
		return;
	}
	tmp->conn_fd = fd;
	*client = tmp;
}

//this is where client sends request, you sould write your code here
int csiebox_client_run(csiebox_client* client) {
	if (!login(client)) {
		fprintf(stderr, "login fail\n");
		return 0;
	}
	fprintf(stderr, "login success\n");
	
	
	struct hash inodemap ;
	memset(&inodemap,0,sizeof(inodemap)) ;
	struct hash inotify ;
	memset(&inotify,0,sizeof(inotify));
	
	if( !init_hash(&inotify,10000) ){
		fprintf(stderr,"init inotify error\n") ;
	}
	
	if( !init_hash( &inodemap, 10000) ){
		fprintf(stderr,"init inodemap error\n") ;
	}
	int ifd = inotify_init();
	
	//fprintf(stderr,"%s\n",client->arg.path);

	int cwd = inotify_add_watch(ifd,client->arg.path,IN_CREATE | IN_DELETE | IN_ATTRIB | IN_MODIFY);
	
	put_into_hash(&inotify, (void*)client->arg.path, cwd );
	
	struct dirent* content ;
	DIR* cdirp = opendir( client->arg.path ) ;
	int num = 0 ;
	//check client dir
	while( (content = readdir( cdirp )) != NULL ){
		if( strcmp(content->d_name,".")!= 0 && strcmp(content->d_name,"..") != 0 )
			num++;
		if( num > 2 )
			break ;
	}
	rewinddir(cdirp) ;
	
		
	//fprintf(stderr,"%d\n",num) ;
	//client dir is empty
	if( num == 0 ){
		download(client,&inotify,ifd);
	}

	else{
		tree_walk(client,client->arg.path,NULL,&inodemap,&inotify,ifd);
	}
	inotify_file( client, ifd , &inotify, cwd, num );
	inotify_rm_watch(ifd,cwd);

	close(ifd);
	return 1;
}

void inotify_file( csiebox_client* client, int ifd, struct hash* inotify, int cwd, int num ){
	int length, i = 0 ;
	char buffer[EVENT_BUF_LEN];
	memset(buffer,0,EVENT_BUF_LEN);
	//clear event
	
	if(num == 0){
		read(ifd, buffer, EVENT_BUF_LEN);
		memset( buffer, 0, sizeof(buffer) );
	}
	
	while ((length = read(ifd, buffer, EVENT_BUF_LEN)) > 0) {
		i = 0 ;
		while (i < length) {
			struct inotify_event* event = (struct inotify_event*)&buffer[i];
			
			char* dirname = NULL;
			char* filename = (char*)malloc(PATH_MAX);
			memset(filename,0,sizeof(filename));

			if( event->wd != cwd )
				get_from_hash( inotify, (void**)&dirname, event->wd );
			
			char fullname[1024] = {0};
			strcat(fullname,client->arg.path);
			strcat(fullname,"/");
			
			if( dirname != NULL ){
				strcat(fullname, dirname);
				strcat(filename,dirname);
			}
			
			strcat(fullname,event->name);
			strcat(filename,event->name);
			
			/*fprintf(stderr,"%s\n",event->name);*/
			//fprintf(stderr,"%s\n",fullname);
			
			
			if ( (event->mask & IN_CREATE) || (event->mask & IN_MODIFY) ) {
				struct stat stat ;
				memset(&stat,0,sizeof(stat));
				if( lstat( fullname, &stat) != 0 ){
					fprintf(stderr,"fail to load stat in inotify\n");
				}
				
				if( event->mask & IN_CREATE && event->mask & IN_ISDIR ){
					int wd = inotify_add_watch(ifd,fullname,IN_CREATE | IN_DELETE | IN_ATTRIB | IN_MODIFY) ;
					strcat(filename,"/");
					put_into_hash(inotify,(void*)filename,wd);
				}
				
				
				//apply shared lock on REG
				
				FILE* fp ;
				struct flock slock ;
				memset(&slock,0,sizeof(slock));
				
				if( !( event->mask & IN_ISDIR ) ){
					fp = fopen(fullname,"r") ;
					slock.l_type = F_RDLCK ;
					slock.l_whence = SEEK_SET;
					slock.l_start = 0 ;
					slock.l_len = 0 ;
					slock.l_pid = getpid();
					if ( fcntl(fileno(fp),F_SETLK,&slock) == -1 ){
						fprintf(stderr,"fail to set shared lock\n");
					}
				}
				if( sync_meta(client,filename,&stat) == 2){

					fseek(fp,0,SEEK_SET) ;
					char buf[1024] = {0} ;
					int size = (int)stat.st_size ;
					sync_file( client, buf, size ) ;

					for( int i = size ; i > 0 ; i -= 1024 ){
						if( i < 1024){
							fread( buf, sizeof(char), i, fp ) ;
							//fprintf(stderr,buf) ;
							//fprintf(stderr,"\n") ;
							if( !send_message( client->conn_fd, buf, i ) ){
								fprintf(stderr,"send file content fail\n") ;
							}
						}
						else{
							fread( buf, sizeof(char), 1024, fp ) ;
							if( !send_message( client->conn_fd, buf, 1024 ) ){
								fprintf(stderr,"send file content fail\n") ;
							}
						}

					}
					//release lock
					slock.l_type = F_UNLCK ;
					if( fcntl(fileno(fp),F_SETLK,&slock) == -1 ){
						fprintf(stderr,"fail to release lock\n");
					}
					fclose(fp);
				}
			}
			if (event->mask & IN_DELETE) {
				rm_file(client,filename);
			}
			if (event->mask & IN_ATTRIB) {
				struct stat stat ;
				memset(&stat,0,sizeof(stat));
				if( lstat( fullname, &stat) != 0 ){
					fprintf(stderr,"fail to load stat in inotify\n");
				}
				sync_meta(client,filename,&stat);
			}

			i += EVENT_SIZE + event->len;
		}
		memset(buffer, 0, EVENT_BUF_LEN);
	}
	close(ifd);
}



void tree_walk( csiebox_client* client, char* clientdir ,char* dirpath, struct hash* inodemap, struct hash* inotify, int ifd ){
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
				  fprintf(stderr,"%s\n",fullname) ;*/

				if( lstat(fullname, &stat) != 0 ){
					fprintf(stderr,"fail to load stat\n") ;
				}

				//check hard link
				int hlink = 0 ;
				char* tgt =NULL ;
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

					int metares = sync_meta( client,filename, &stat ) ;

					if( !metares ){
						fprintf( stderr, "sync_meta error\n" ) ;
					}

					strcat(filename,"/") ;
					int wd = inotify_add_watch(ifd,fullname,IN_CREATE | IN_DELETE | IN_ATTRIB | IN_MODIFY );

					put_into_hash( inotify, (void*)filename, wd);

					tree_walk( client, clientdir ,filename, inodemap, inotify,ifd )  ;

				}
				if( content->d_type == DT_REG ){	
					if( hlink ){
						hardlink( client, tgt, filename ) ;
					}
					else{
						//apply shared lock
						FILE* fp = fopen(fullname,"r") ;
						struct flock slock ;
						memset(&slock,0,sizeof(slock));
						slock.l_type = F_RDLCK ;
						slock.l_whence = SEEK_SET;
						slock.l_start = 0 ;
						slock.l_len = 0 ;
						slock.l_pid = getpid();
						if ( fcntl( fileno(fp),F_SETLK,&slock ) == -1 ){
							fprintf(stderr,"fail to set shared lock \n");
						}

						int metares = sync_meta( client, filename, &stat ) ;	
						if( !metares ){
							fprintf( stderr, "sync_meta error\n" ) ;
						}

						if(metares == 2){

							fseek(fp,0,SEEK_SET) ;
							char buf[1024] = {0} ;
							int size = (int)stat.st_size ;
							sync_file( client, buf, size ) ;

							for( int i = size ; i > 0 ; i -= 1024 ){
								if( i < 1024){
									fread( buf, sizeof(char), i, fp ) ;
									//fprintf(stderr,buf) ;
									//fprintf(stderr,"\n") ;
									if( !send_message( client->conn_fd, buf, i ) ){
										fprintf(stderr,"send file content fail\n") ;
									}
								}
								else{
									fread( buf, sizeof(char), 1024, fp ) ;
									if( !send_message( client->conn_fd, buf, 1024 ) ){
										fprintf(stderr,"send file content fail\n") ;
									}
								}

							}
						}
						//release lock
						slock.l_type = F_UNLCK ;
						if( fcntl(fileno(fp),F_SETLK,&slock) == -1 ){
							fprintf(stderr,"fail to release lock\n");
						}
						fclose(fp);
					}
				}
				if( content->d_type == DT_LNK ){
					if( sync_meta( client, filename, &stat ) == 2 ){
						char buf[PATH_MAX] = {0};
						int size = (int)stat.st_size ;

						sync_file( client, buf, size ) ;

						readlink( fullname, buf, PATH_MAX ) ;
						if( size < PATH_MAX )
							buf[size]= '\0' ;
						else
							fprintf(stderr,"symlink too long\n");

						if( !send_message( client->conn_fd, buf, PATH_MAX ) )
							fprintf(stderr,"send symlink content fail\n") ;
					}
				}			
			}
		}
	}
	closedir(dirp);
}

void csiebox_client_destroy(csiebox_client** client) {
	csiebox_client* tmp = *client;
	*client = 0;
	if (!tmp) {
		return;
	}
	close(tmp->conn_fd);
	free(tmp);
}

//read config file
static int parse_arg(csiebox_client* client, int argc, char** argv) {
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
	int accept_config_total = 5;
	int accept_config[5] = {0, 0, 0, 0, 0};
	while ((keylen = getdelim(&key, &keysize, '=', file) - 1) > 0) {
		key[keylen] = '\0';
		vallen = getline(&val, &valsize, file) - 1;
		val[vallen] = '\0';
		fprintf(stderr, "config (%d, %s)=(%d, %s)\n", keylen, key, vallen, val);
		if (strcmp("name", key) == 0) {
			if (vallen <= sizeof(client->arg.name)) {
				strncpy(client->arg.name, val, vallen);
				accept_config[0] = 1;
			}
		} else if (strcmp("server", key) == 0) {
			if (vallen <= sizeof(client->arg.server)) {
				strncpy(client->arg.server, val, vallen);
				accept_config[1] = 1;
			}
		} else if (strcmp("user", key) == 0) {
			if (vallen <= sizeof(client->arg.user)) {
				strncpy(client->arg.user, val, vallen);
				accept_config[2] = 1;
			}
		} else if (strcmp("passwd", key) == 0) {
			if (vallen <= sizeof(client->arg.passwd)) {
				strncpy(client->arg.passwd, val, vallen);
				accept_config[3] = 1;
			}
		} else if (strcmp("path", key) == 0) {
			if (vallen <= sizeof(client->arg.path)) {
				strncpy(client->arg.path, val, vallen);
				accept_config[4] = 1;
			}
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

static int login(csiebox_client* client) {
	csiebox_protocol_login req;
	memset(&req, 0, sizeof(req));
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_LOGIN;
	req.message.header.req.datalen = sizeof(req) - sizeof(req.message.header);
	memcpy(req.message.body.user, client->arg.user, strlen(client->arg.user));
	md5(client->arg.passwd,
			strlen(client->arg.passwd),
			req.message.body.passwd_hash);
	if (!send_message(client->conn_fd, &req, sizeof(req))) {
		fprintf(stderr, "send fail\n");
		return 0;
	}
	csiebox_protocol_header header;
	memset(&header, 0, sizeof(header));
	if (recv_message(client->conn_fd, &header, sizeof(header))) {
		if (header.res.magic == CSIEBOX_PROTOCOL_MAGIC_RES &&
				header.res.op == CSIEBOX_PROTOCOL_OP_LOGIN &&
				header.res.status == CSIEBOX_PROTOCOL_STATUS_OK) {
			client->client_id = header.res.client_id;
			return 1;
		} else {
			return 0;
		}
	}
	return 0;
}


static int sync_meta(csiebox_client* client, char* filepath, struct stat* stat ){
	
	//check whether server is busy ;
	csiebox_protocol_header check ;
	memset(&check,0,sizeof(check));
	check.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ;
	check.req.op = 0x78 ;
	check.req.client_id = client->client_id ;
	check.req.datalen = sizeof(check);
	
	label:
	//send chech_req
	send_message( client->conn_fd, &check, sizeof(check) );
	
	//recv check res
	csiebox_protocol_header check_res ;
	memset(&check_res,0,sizeof(check_res));
	
	while( recv_message( client->conn_fd, &check_res, sizeof(check_res)) ){
		if( check_res.res.status == 0x04 ){
			fprintf(stdout,"server busy\n");
			fflush(stdout);
			sleep(3);
			//re-send message
			send_message( client->conn_fd, &check, sizeof(check) );
		}
		else if( check_res.res.status == 0x05 )
			break ;
	}
	
	csiebox_protocol_meta req ;
	memset(&req,0,sizeof(req)) ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
	req.message.header.req.client_id = client->client_id ;
	req.message.header.req.datalen = sizeof(req) - sizeof(req.message.header) ;
	req.message.body.pathlen = strlen(filepath) ;

	req.message.body.stat = *stat;
	/*fprintf(stderr,"%d\n",(int)stat->st_size);
	  fprintf(stderr,"%d\n",req.message.body.stat.st_size);*/
	char fullname[PATH_MAX] = {0};
	strcat(fullname,client->arg.path);
	strcat(fullname,"/");
	strcat(fullname,filepath);



	//fprintf(stderr,"%s\n",fullname);

	if( S_ISREG(stat->st_mode) )
		md5_file( fullname, req.message.body.hash ) ;
	
	//send meta
	if( !send_message( client->conn_fd, &req, sizeof(req) ) ){
		fprintf(stderr,"send fail\n") ;
		return 0 ;
	}
	
	//send filepath
	
	if( !send_message( client->conn_fd, filepath, 1024 ) ){
		fprintf(stderr,"send filepath fail\n") ;
	}
	
	//check block
	int block ;
	if(recv_message(client->conn_fd, &block, sizeof(block) )){
		if(block == 1){
			fprintf(stdout,"server block\n");
			fflush(stdout);
			sleep(3);
			goto label ;
		}
	}
	
	csiebox_protocol_header header ;
	memset( &header, 0 ,sizeof(header) ) ;
	//recv res
	if( recv_message(client->conn_fd, &header, sizeof(header) ) ){
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

static int sync_file( csiebox_client* client, char* buffer, int size ){	
	csiebox_protocol_file req ;
	memset(&req,0,sizeof(req)) ;
	req.message.body.datalen = size ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_FILE ;
	req.message.header.req.client_id = client->client_id ;

	if( !send_message( client->conn_fd, &req, sizeof(req) ) )
		fprintf(stderr,"send datalen fail") ;

	return 1 ;
}
static int hardlink( csiebox_client* client, char* tgt, char* src ){
	csiebox_protocol_hardlink req ;
	memset( &req, 0, sizeof(req) ) ;
	req.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	req.message.header.req.op = CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK ;
	req.message.header.req.client_id = client->client_id ;
	req.message.header.req.datalen = sizeof(req)-sizeof(req.message.header) ;

	/*fprintf(stderr,"tgt and src\n");
	  fprintf(stderr,"%s\n",tgt);
	  fprintf(stderr,"%s\n",src);*/

	req.message.body.srclen = strlen(src)+1 ;
	req.message.body.targetlen = strlen(tgt)+1 ;
	//send hardlink req
	if( !send_message( client->conn_fd, &req, sizeof(req) ) )
		fprintf(stderr,"send hardlink fail\n");

	//send target path
	//fprintf(stderr,"%d\n",strlen(tgt));
	if( !send_message( client->conn_fd, tgt, strlen(tgt)+1 ) ){
		fprintf(stderr,"send target fail\n");
		return 0 ;
	}
	//send source path
	//fprintf(stderr,"%d\n",strlen(src));
	if( !send_message( client->conn_fd, src, strlen(src)+1 ) ){
		fprintf(stderr,"send source fail\n") ;
		return 0 ;
	}
	//recv result
	csiebox_protocol_header header ;
	memset( &header, 0, sizeof(header) ) ;
	if( recv_message(client->conn_fd, &header, sizeof(header)) ) {
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

static int rm_file( csiebox_client* client, char* name ){
	csiebox_protocol_rm rm ;
	memset( &rm, 0, sizeof(rm) ) ;
	rm.message.header.req.magic = CSIEBOX_PROTOCOL_MAGIC_REQ ;
	rm.message.header.req.op = CSIEBOX_PROTOCOL_OP_RM ;
	rm.message.header.req.datalen = sizeof(rm)-sizeof(rm.message.header);
	rm.message.header.req.client_id = client->client_id ;
	rm.message.body.pathlen = strlen(name) + 1 ;

	//fprintf(stderr,"%d\n",rm.message.body.pathlen);

	//send rm req
	if( !send_message( client->conn_fd, &rm, sizeof(rm) ) ){
		fprintf(stderr,"fail to send rm req\n");
	}
	//send rm file name
	if( !send_message( client->conn_fd, name, strlen(name)+1 )  ){
		fprintf(stderr,"fail to send rm file name\n");
	}
	csiebox_protocol_header header ;
	memset( &header, 0, sizeof(header) ) ;

	if( recv_message( client->conn_fd, &header, sizeof(header) ) ){
		if( header.res.status == CSIEBOX_PROTOCOL_STATUS_OK )
			return 1 ;
		else
			return 0 ;
	}
	return 0 ;
}
//download
static void download(csiebox_client* client, struct hash* inotify, int ifd ) {
	csiebox_protocol_header header;
	memset(&header, 0, sizeof(header));
	int stop = 0 ;
	while( !stop && recv_message(client->conn_fd, &header, sizeof(header)) ) {
		if (header.req.magic == CSIEBOX_PROTOCOL_MAGIC_REQ) { 
			switch (header.req.op) {
				case 0x87:
					stop = 1 ;
					break;
				case CSIEBOX_PROTOCOL_OP_SYNC_META:
					fprintf(stderr, "sync meta\n");
					csiebox_protocol_meta meta;
					if (complete_message_with_header(client->conn_fd, &header, &meta)) {
						if( !d_sync_meta( client ,&meta, inotify, ifd ) ){
							fprintf(stderr,"sync meta error\n") ;
						}
					}
					break;
				case CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK:
					fprintf(stderr, "sync hardlink\n");
					csiebox_protocol_hardlink hardlink;
					if (complete_message_with_header(client->conn_fd, &header, &hardlink)) {
						d_sync_hardlink( client, &hardlink, inotify, ifd );
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
					if (complete_message_with_header(client->conn_fd, &header, &rm)) {
						d_rm_file( client, &rm ) ;
					}
					break;
				default:
					fprintf(stderr, "unknown op %x\n", header.req.op);
					break;
			}
		}
	}
}
static int d_sync_meta( csiebox_client* client,  csiebox_protocol_meta* meta, struct hash* inotify, int ifd ){

	char buf[1024] = {0} ;
	//recv file path

	if( recv_message( client->conn_fd, buf, sizeof(buf) ) ){
		char filename[1024] = {0} ;
		int exist = 1 ;
		int mod = 0 ;

		memcpy( filename, buf, meta->message.body.pathlen ) ;

		char dirname[PATH_MAX] ={0} ;
		strcat(dirname,client->arg.path) ;

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
		if( exist ){
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
			}
		}
		csiebox_protocol_header header ;
		memset(&header,0,sizeof(header)) ;

		if( mod || !exist ){
			header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
			header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
			if( S_ISDIR(meta->message.body.stat.st_mode) )
				header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
			else	
				header.res.status = CSIEBOX_PROTOCOL_STATUS_MORE ;

			send_message( client->conn_fd, &header, sizeof(header) ) ;

			if( !d_sync_file(client, dirname ,filename , &meta->message.body.stat, inotify, ifd ) ){
				fprintf(stderr,"sync file fail\n") ;
				return 0 ;
			}

			return 1;
		}
		else{
			header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
			header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_META ;
			header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
			send_message( client->conn_fd, &header, sizeof(header) ) ;
			return 1;
		}
	}
	return 0;
}


static int d_sync_file( csiebox_client* client, char* dirname, char* filename, struct stat* stat, struct hash* inotify, int ifd ){
	csiebox_protocol_file file ;
	memset(&file,0,sizeof(file)) ;

	char fullname[1024] ={0};
	strcat(fullname,dirname);
	strcat(fullname,filename);
	if(S_ISDIR(stat->st_mode)){
		mkdir(fullname,DIR_S_FLAG);

		//add new dir into inotify
		int wd = inotify_add_watch(ifd, fullname,IN_CREATE | IN_DELETE | IN_ATTRIB | IN_MODIFY );
		char* hashname = (char*)malloc(1024);
		memset( hashname, 0, sizeof(hashname) );
		strcat( hashname, filename);

		put_into_hash( inotify, (void*)hashname, wd);
	}
	//recv total datalen
	else if( recv_message( client->conn_fd, &file, sizeof(file) ) ){
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
				FILE* fp ; 
				if( !(fp=fopen( fullname, "w+" )) ){
					fprintf(stderr,"fail to open file\n");
				}
				//fprintf(stderr,filename) ;
				/*fprintf(stderr,"\n");
				  getcwd(buf,PATH_MAX) ;
				  fprintf(stderr,buf) ;*/
				for( int i = size ; i >0; i-= sizeof(buf)  ){
					int rbyte = 0 ;
					if(i < 1024){	
						recv_message( client->conn_fd, buf, i );
						rbyte = i ;
					}
					else{
						recv_message(client->conn_fd, buf, sizeof(buf));
						rbyte = sizeof(buf) ;
					}
					fwrite( buf, sizeof(char), rbyte, fp ) ;
				}	
				fclose(fp) ;
			}
			if( S_ISLNK(stat->st_mode) ){
				//content of link should < PATH_MAX
				char tgt[PATH_MAX] = {0} ;
				recv_message( client->conn_fd, tgt, PATH_MAX );

				//fprintf(stderr,"%s\n",tgt);

				unlink( fullname );
				if( symlink(tgt,fullname) != 0 ){
					fprintf(stderr,"creat symlnk fail\n");
				}
			}
		}
	}
	//sync time
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


	return 1 ;
}
static void d_sync_hardlink( csiebox_client* client, csiebox_protocol_hardlink* hardlink, struct hash* inotify, int ifd ){
	char tgtf[1024]={0} ;
	char srcf[1024]={0} ;
	char buf[1024]={0} ;
	char* src = (char*)malloc(1024);
	memset(src,0,sizeof(src));

	char dirname[PATH_MAX] = {0};
	strcat( dirname, client->arg.path ) ;

	strcat(dirname,"/") ;

	strcat( tgtf, dirname ) ;
	strcat( srcf, dirname ) ;

	/*fprintf(stderr,"%d\n",hardlink->message.body.targetlen);
	  fprintf(stderr,"%d\n",hardlink->message.body.srclen);

	  fprintf(stderr,"%s\n",client->arg.path) ;
	  fprintf(stderr,"%s\n",tgt);
	  fprintf(stderr,"%s\n",src) ;*/

	//recv target name
	if( recv_message( client->conn_fd, &buf, hardlink->message.body.targetlen) ){
		strcat( tgtf, buf ) ;
	}
	//recv source name
	if( recv_message(client->conn_fd, &buf, hardlink->message.body.srclen ) ){
		strcat( src,buf );
		strcat( srcf, buf ) ;
	}

	/*fprintf(stderr,"tgt and src\n");
	  fprintf(stderr,"%s\n",tgt);
	  fprintf(stderr,"%s\n",src) ;*/

	csiebox_protocol_header header ;
	memset(&header,0,sizeof(header)) ;
	header.res.magic = CSIEBOX_PROTOCOL_MAGIC_RES ;
	header.res.op = CSIEBOX_PROTOCOL_OP_SYNC_HARDLINK ;
	header.res.datalen = 0 ;
	if( link( tgtf, srcf)  == 0 ){
		header.res.status = CSIEBOX_PROTOCOL_STATUS_OK ;
	}
	else{
		header.res.status = CSIEBOX_PROTOCOL_STATUS_FAIL ;
		fprintf(stderr,"sync hardlink fail\n");
	}
	send_message( client->conn_fd, &header, sizeof(header) ) ;
	//add into inotify

	/*int wd = inotify_add_watch(ifd, srcf, IN_CREATE | IN_DELETE | IN_ATTRIB | IN_MODIFY );
	  put_into_hash( inotify, (void*)src, wd );*/

}
static void d_rm_file( csiebox_client* client, csiebox_protocol_rm* rm ){
	char fullname[1024]={0} ;
	char buf[1024]={0} ;

	char dirname[PATH_MAX] ={0} ;
	strcat( dirname,client->arg.path) ;
	strcat( dirname, "/" ) ;
	strcpy( fullname, dirname );
	//recv name

	if( recv_message( client->conn_fd, buf, rm->message.body.pathlen ) ){
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
	send_message( client->conn_fd, &header, sizeof(header) ) ;
}
