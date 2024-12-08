#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <csignal>
#include <thread>
#include <random>

#include "listener.h"
#include "worker.h"

#define PORT "8080"
#define MAXBUFLEN 4400

#ifdef DEBUG
#define DBG(x) x
#else
#define DBG(x)
#endif

// get sockaddr, IPv4 or IPv6:
void *getInAddr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

void runUdpServer(FullNode& node) {
	int sockfd;  // listen on sock_fd
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_storage their_addr; // connector's address information
	char buf[MAXBUFLEN];
	socklen_t addr_len;
	int rv;
  int numbytes;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

	if ((rv = getaddrinfo(NULL, PORT, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return;
	}

	// loop through all the results and bind to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("listener: socket");
			continue;
		}

		if (::bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("listener: bind");
			continue;
		}

		break;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (p == NULL)  {
		fprintf(stderr, "server: failed to bind\n");
		exit(1);
	}

	DBG(printf("listener: waiting for recvfrom...\n"));

	while(1) {  // main accept() loop
		addr_len = sizeof their_addr;
    if ((numbytes = recvfrom(sockfd, buf, MAXBUFLEN-1 , 0,
			(struct sockaddr *)&their_addr, &addr_len)) == -1) {
			perror("recvfrom");
			exit(1);
		}

		DBG(printf("listener: got packet from %s\n",
			inet_ntop(their_addr.ss_family,
				getInAddr((struct sockaddr *)&their_addr),
				s, sizeof s)));
		DBG(printf("listener: packet is %d bytes long\n", numbytes));
		buf[numbytes] = '\0';
		DBG(printf("listener: packet contains \"%s\"\n", buf));

		if (node.getDropRate() > 0) {
			random_device rd;
			mt19937 gen(rd());
			bernoulli_distribution dropDist(node.getDropRate());
			if (dropDist(gen)) {
				DBG(printf("listener: dropped message\n"));
				continue;
			}
		}
		handleRequest(node, buf);
		// std::thread requestThread(handleRequest, std::ref(node), buf);
		// requestThread.detach();
	}

	close(sockfd);
	return;
}

