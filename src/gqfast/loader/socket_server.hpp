#ifndef socket_server_
#define socket_server_

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>
#include <unistd.h>

#include "load.hpp"
#include "input_handling.hpp"
#include "global_vars.hpp"

#define BUFFER_SIZE 1024
#define PORT_NO 7235


int read_from_socket(char contentBuffer[], char buffer[], int clisockfd)
{
    int n=-1;

    bzero(buffer,BUFFER_SIZE);
    bzero(contentBuffer,BUFFER_SIZE-1);

    n = read( clisockfd,buffer,BUFFER_SIZE-1);
    if (n < 0)
    {
        perror("ERROR reading from socket");
        return(1);
    }

    strncpy(contentBuffer,buffer,strlen(buffer) - 1);
    return 0;
}

int write_to_socket(int clisockfd, string messageString) {

    int n=-1;
    const char* message = messageString.c_str();

 //   strcpy(message, messageString.c_str());
    n = write(clisockfd, message, messageString.length());
  //  delete[] message;
    if (n < 0)
    {
        perror("ERROR writing to socket");
        return(1);
    }
    return 0;
}


int java_program_communicator()
{



    int sockfd, clisockfd, portno;
    char * start = "Message init";
    char * end = "Message terminate";
    char * shutdown = "Shutdown signal";
    char * shutdown_accepted = "Shutdown response received";
    socklen_t clilen;
    char buffer[BUFFER_SIZE];
    char contentBuffer[BUFFER_SIZE-1];
    struct sockaddr_in serv_addr, cli_addr;

    string out_message;
    //int optval;

    /* First call to socket() function */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        perror("ERROR opening socket");
        return(1);
    }

    /* Initialize socket structure */
    bzero((char *) &serv_addr, sizeof(serv_addr));
    portno = PORT_NO;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);


    if (bind(sockfd, (struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0)
    {
        perror("ERROR on binding");
        return(1);
    }

    listen(sockfd,5);
    clilen = (socklen_t) sizeof(cli_addr);

    clisockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);

    if (clisockfd < 0)
    {
        perror("ERROR on accept");
        return(1);
    }

    while (1)
    {
        out_message = "Waiting for message\n";
        if(write_to_socket(clisockfd,out_message)) {
            return 1;
        }
        cerr << "in first loop\n";
        if(read_from_socket(contentBuffer, buffer, clisockfd)) {
            return 1;
        }
        if (strcmp(start, contentBuffer) ==0)
        {
            while (strcmp(end, contentBuffer) !=0)
            {
                out_message = "Continue message\n";
                if (write_to_socket(clisockfd, out_message)) {
                    return 1;
                }

                if (read_from_socket(contentBuffer, buffer, clisockfd)) {
                    return 1;
                }

                cerr << "in while loop\n";

            }

            out_message = "Message complete\n";
            if (write_to_socket(clisockfd, out_message)) {
                return 1;
            }

        }
        else if (strcmp(shutdown, contentBuffer) ==0)
        {
            out_message = "Server shut down\n";
            if (write_to_socket(clisockfd,out_message)) {
                return 1;
            }
            break;
        }
        else
        {
            printf("Unknown command: %s\n",buffer);
            out_message = "ERRCmd\n";
            if (write_to_socket(clisockfd,out_message)) {
                return 1;
            }
        }
    }

    close(sockfd);
    return 0;

}



#endif
