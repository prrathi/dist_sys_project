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
#include "listener.h"
#include "worker.h"

#define PORT "8080"
#define MAXBUFLEN 4400

#ifdef DEBUG
#define DBG(x) x
#else
#define DBG(x)
#endif

using namespace std;

void sendUdpRequest(string hostname, string message) {
  int sockfd;
	struct addrinfo hints, *servinfo, *p;
	int rv;
	int numbytes;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC; // set to AF_INET to use IPv4
	hints.ai_socktype = SOCK_DGRAM;

	if ((rv = getaddrinfo(hostname.c_str(), PORT, &hints, &servinfo)) != 0) {
		DBG(fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv)));
		return;
	}

	// loop through all the results and make a socket
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			DBG(perror("talker: socket"));
			continue;
		}

		break;
	}

	freeaddrinfo(servinfo);

	if (p == NULL) {
		DBG(fprintf(stderr, "talker: failed to create socket\n"));
		return;
	}

	if (message.length() >= MAXBUFLEN) {
		DBG(fprintf(stderr, "talker: message length exceeds MAXBUFLEN\n"));
		close(sockfd);
		return;
	}

	if ((numbytes = sendto(sockfd, message.c_str(), message.length(), 0,
			 p->ai_addr, p->ai_addrlen)) == -1) {
		DBG(perror("talker: sendto"));
		return;
	}

	DBG(printf("talker: sent %d bytes to %s\n", numbytes, hostname.c_str()));
	close(sockfd);
}

