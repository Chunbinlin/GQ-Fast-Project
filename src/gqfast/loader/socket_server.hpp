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




uint32_t read_int_from_socket(char contentBuffer[], char buffer[], int clisockfd)
{
    int n=-1;

    bzero(buffer,BUFFER_SIZE);
    bzero(contentBuffer,BUFFER_SIZE-1);

    n = recv(clisockfd,buffer,BUFFER_SIZE-1, MSG_WAITALL);
    if (n < 0)
    {
        perror("ERROR reading from socket");
        exit(0);
    }
    uint32_t raw32;

    strncpy(contentBuffer, buffer, n);
    memcpy(&raw32,contentBuffer, 4);
    cerr << raw32 << ", " << contentBuffer << "\n";
    uint32_t readInt = ntohl(raw32);

    return readInt;
}


int read_string_from_socket(char contentBuffer[], char buffer[], int clisockfd)
{

    int n=-1;

    bzero(buffer,BUFFER_SIZE);
    bzero(contentBuffer,BUFFER_SIZE-1);

    n = read(clisockfd,buffer,BUFFER_SIZE-1);
    if (n < 0)
    {
        perror("ERROR reading from socket");
        exit(0);
    }

    strncpy(contentBuffer, buffer, n);

    return n;
}



void write_to_socket(int clisockfd, string messageString)
{

    int n=-1;
    const char* message = messageString.c_str();

    uint32_t length = messageString.length();
    length = htonl(length);

    n = write(clisockfd, (const char*)&length, 4);
    //  delete[] message;
    if (n < 0)
    {
        perror("ERROR writing to socket");
        exit(0);
    }

//   strcpy(message, messageString.c_str());
    n = write(clisockfd, message, messageString.length());
    //  delete[] message;
    if (n < 0)
    {
        perror("ERROR writing to socket");
        exit(0);
    }

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

    char* out_message;
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
        write_to_socket(clisockfd,out_message);
        read_string_from_socket(contentBuffer, buffer, clisockfd);

        if (strcmp(start, contentBuffer) ==0)
        {
            while (strcmp(end, contentBuffer) !=0)
            {

                read_string_from_socket(contentBuffer, buffer, clisockfd);

                string tempContentString(contentBuffer);
                cerr << tempContentString << "\n";
                if (server_command_map[tempContentString] == iload_begin)
                {
                    read_string_from_socket(contentBuffer, buffer, clisockfd);
                    string filename(contentBuffer);
                    uint32_t num_encodings = read_int_from_socket(contentBuffer, buffer, clisockfd);
                    cerr << "num encodings = " << num_encodings << "\n";

                    char* col_names[num_encodings];
                    uint32_t encodings[num_encodings];
                    for (uint32_t i=0; i<num_encodings; i++)
                    {
                        int n = read_string_from_socket(contentBuffer, buffer, clisockfd);
                        char* temp = new char[n];
                        strcpy(temp, contentBuffer);
                        col_names[i] = temp;
                        encodings[i] = read_int_from_socket(contentBuffer, buffer, clisockfd);
                    }
                    cerr << "filename = " << filename << "\n";
                    cerr << "colname[0] = " << col_names[0] << "\n";
                    cerr << "encodings[0] = " << encodings[0] << "\n";

                    for (uint32_t i=0; i<num_encodings; i++)
                    {
                        delete[] col_names[i];
                    }
                }


            }

            out_message = "Message complete\n";
            write_to_socket(clisockfd, out_message);

        }
        else if (strcmp(shutdown, contentBuffer) ==0)
        {
            out_message = "Server shut down\n";
            write_to_socket(clisockfd,out_message);
            break;
        }
        else
        {
            printf("Unknown command: %s\n",buffer);
            out_message = "ERRCmd\n";
            write_to_socket(clisockfd,out_message);
        }
    }

    close(sockfd);
    return 0;

}



#endif
