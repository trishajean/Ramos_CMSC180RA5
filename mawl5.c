#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 4096
#define MAX_THREADS 64
#define MAX_IP_LENGTH 16
#define MAX_FILE_LINE_LENGTH 256

typedef struct{
    int start;
    int end;
    int n;
    int* matrix_x;
    int* vector_y;
    int thread_id;
    int core_id;
    int columns;
    int port;
    char* ip;
} Thread_Args;

typedef struct{
    int start;
    int end;
    int* array;
    int core_id;
} Thread_Args_Array;

int read_slave_ips_from_file(const char* filename, char** ips, int max_slaves) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open IP file");
        return -1;
    }
    
    int count = 0;
    char line[MAX_FILE_LINE_LENGTH];
    
    while (fgets(line, sizeof(line), file) && count < max_slaves) {
        // Remove newline character if present
        size_t len = strlen(line);
        if (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) {
            line[len-1] = '\0';
            if (len > 1 && (line[len-2] == '\r')) {
                line[len-2] = '\0';
            }
        }
        
        // Skip empty lines and comments
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }
        
        // Copy IP address
        ips[count] = (char*)malloc(MAX_IP_LENGTH * sizeof(char));
        if (ips[count] == NULL) {
            perror("Memory allocation failed for IP address");
            fclose(file);
            
            // Free previously allocated memory
            for (int i = 0; i < count; i++) {
                free(ips[i]);
            }
            return -1;
        }
        
        strncpy(ips[count], line, MAX_IP_LENGTH - 1);
        ips[count][MAX_IP_LENGTH - 1] = '\0';
        
        count++;
    }
    
    fclose(file);
    return count;
}

int* create_array(int n){
    int *arr = (int *)malloc(n * n * sizeof(int));
    for(int i = 0; i < n * n; i++){
        arr[i] = rand() % 10 + 1;

    }

    return arr;
}

int* create_vector(int n){
    int *arr = (int *)malloc(n * sizeof(int));
    for(int i = 0; i < n; i++){
        arr[i] = rand() % 10 + 1;
    }

    return arr;
}

void free_array(int* arr){
    free(arr);
}

void receive_submatrix_from_master(int port, int n, int columns, int *submatrix, int *vector_y){
    int sockfd, newsockfd;
    struct sockaddr_in serv_addr, cli_addr;
    socklen_t clilen;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        perror("Socket creation error");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
        perror("Binding failed!");
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    if (listen(sockfd, 1) < 0) {
        perror("Listening failed!");
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Slave listening on port %d...\n", port);
    
    clilen = sizeof(cli_addr);
    if ((newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen)) < 0) {
        perror("Accepting failed!");
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(cli_addr.sin_addr), client_ip, INET_ADDRSTRLEN);
    printf("Connection accepted from %s:%d\n", client_ip, ntohs(cli_addr.sin_port));

    for(int i = 0; i < columns; i++){
        int bytes_received = 0;
        int *temp = submatrix + i * n;
        while (bytes_received < n * sizeof(int)){
            int remaining_bytes = n * sizeof(int) - bytes_received;
            int bytes = recv(newsockfd, (char*)(temp) + bytes_received, remaining_bytes, 0);
            if (bytes == -1){
                perror("Receiving failed!");
                close(newsockfd);
                close(sockfd);
                exit(EXIT_FAILURE);
            } else if (bytes == 0){
                fprintf(stderr, "Connection closed prematurely!\n");
                close(newsockfd);
                close(sockfd);
                exit(EXIT_FAILURE);
            }
            bytes_received += bytes;
        }
    }

    printf("Submatrix received from master!\n");

    if (send(newsockfd, "ack", 3, 0) == -1){
        perror("Send ack failed!");
        exit(EXIT_FAILURE);
    }

    int bytesReceived = 0;
    while (bytesReceived < n * sizeof(int)) {
        int remainingBytes = n * sizeof(int) - bytesReceived;
        int bytes = recv(newsockfd, vector_y + bytesReceived / sizeof(int), remainingBytes, 0);
        if (bytes == -1) {
            perror("Receiving failed!\n");
            close(newsockfd);
            close(sockfd);
            exit(EXIT_FAILURE);
        } else if (bytes == 0) {
            fprintf(stderr, "Connection closed prematurely!\n");
            close(newsockfd);
            close(sockfd);
            exit(EXIT_FAILURE);
        }
        bytesReceived += bytes;
    }

    printf("Vector y received!\n");
    if (send(newsockfd, "ack", 3, 0) == -1) {
        perror("Send acknowledgment failed!");
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    double *r = calloc(columns, sizeof(double));
    struct timeval t1, t2;
    double elapsed;

    gettimeofday(&t1, NULL);
    for (int j = 0; j < columns; j++){
        int summ = 0;
        for (int i = 0; i < n; i++){
            int idx = i * columns + j;
            summ += pow(submatrix[idx] - vector_y[i], 2);
        }
        r[j] = sqrt((double)summ / n);
    }
    gettimeofday(&t2, NULL);

    elapsed = (double)(t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec) / 1000000.0;
    printf("Time to complete MSE computation for this slave: %.8f seconds\n", elapsed);

    if (send(newsockfd, r, sizeof(double) * columns, 0) == -1) {
        perror("Send result failed!");
        free(r);
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    char ack[4];
    if (recv(newsockfd, ack, sizeof(ack), 0) == -1) {
        perror("Receive acknowledgment failed!");
        free(r);
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    } else if (strncmp(ack, "ack", 3) != 0) {
        fprintf(stderr, "Unexpected acknowledgment received!");
        free(r);
        close(newsockfd);
        close(sockfd);
        exit(EXIT_FAILURE);
    } else {
        printf("Vector r sent to master.\n");
    }

    free(r);
    close(newsockfd);
    close(sockfd);
}

void send_submatrix_to_slave(char *ip, int port, int n, int rows, int columns, int start, int* matrix, int* vector_y){
    printf("Sending data to %s:%d\n", ip, port);
    
    int sockfd;
    struct sockaddr_in serv_addr;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        perror("Socket creation error!");
        exit(EXIT_FAILURE);
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0){
        perror("Invalid address/ Address not supported!");
        exit(EXIT_FAILURE);
    }

    // Try to connect with timeout and retry
    int retry_count = 0;
    int max_retries = 100;
    int connected = 0;
    
    while (!connected && retry_count < max_retries){
        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            perror("Connection failed, retrying...");
            retry_count++;
            sleep(2); // Wait 2 seconds before retrying
            close(sockfd);
            
            if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
                perror("Socket recreation error!");
                exit(EXIT_FAILURE);
            }
        } else {
            connected = 1;
        }
    }
    
    if (!connected) {
        fprintf(stderr, "Failed to connect to %s:%d after %d attempts\n", ip, port, max_retries);
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < columns; i++){
        int bytes_sent = 0;
        while (bytes_sent < n * sizeof(int)){
            int remaining = n * sizeof(int) - bytes_sent;
            int bytes = send(sockfd, (char *)(matrix + start + i * n) + bytes_sent, remaining, 0);
            if (bytes == -1){
                perror("Sending failed!");
                exit(EXIT_FAILURE);
            }
            bytes_sent += bytes;
        }
    }

    char ack_buffer[4] = {0};
    if (recv(sockfd, ack_buffer, 3, 0) <= 0) {
        perror("");
    } else if (strcmp(ack_buffer, "ack") == 0) {
        printf("Received acknowledgment from slave\n");
    }

    if (send(sockfd, vector_y, sizeof(int) * rows, 0) == -1) {
        perror("Send vector y failed");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Submatrix sent to %s:%d\n", ip, port);

    if (recv(sockfd, ack_buffer, sizeof(ack_buffer), 0) == -1) {
        perror("Failed to receive acknowledgment");
        close(sockfd);
        exit(EXIT_FAILURE);
    } else {
        printf("Vector y sent to %s:%d\n", ip, port);
    }

    double *r = calloc(columns, sizeof(double));
    int bytesReceived = 0;
    while (bytesReceived < columns * sizeof(double)) {
        int remainingBytes = columns * sizeof(double) - bytesReceived;
        int bytes = recv(sockfd, (char *)r + bytesReceived, remainingBytes, 0);
        if (bytes == -1) {
            perror("Master: Receive result failed");
            free(r);
            close(sockfd);
            exit(EXIT_FAILURE);
        } else if (bytes == 0) {
            fprintf(stderr, "Master: Connection closed prematurely");
            free(r);
            close(sockfd);
            exit(EXIT_FAILURE);
        }
        bytesReceived += bytes;
    }

    printf("Result vector received from %s:%d\n", ip, port);

    if (send(sockfd, "ack", 3, 0) == -1) {
        perror("Send acknowledgment failed");
        free(r);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    free(r);
    close(sockfd);
}

void* distribute_submatrices_to_slaves(void* args){
    Thread_Args* thread_args = (Thread_Args*)args;
    int start = thread_args->start;
    int end = thread_args->end;
    int* matrix_x = thread_args->matrix_x;
    int* vector_y = thread_args->vector_y;
    int n = thread_args->n;
    int thread_id = thread_args->thread_id;
    int columns = thread_args->columns;
    int port = thread_args->port;
    char* ip = thread_args->ip;
    int core_id = thread_args->core_id;
    
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    
    send_submatrix_to_slave(ip, port, n, n, columns, start, matrix_x, vector_y);

    pthread_exit(NULL);
}

void divide_matrix_to_slaves(int* matrix_x, int n, int cols_per_thread, int num_slaves, int base_port, char** ips, int max_cores, int* vector_y){
    pthread_t threads[num_slaves];
    Thread_Args thread_args[num_slaves];

    for (int i = 0; i < num_slaves; i++){
        thread_args[i].columns = cols_per_thread;
        thread_args[i].start = i * cols_per_thread * n;
        thread_args[i].end = (i == num_slaves - 1) ? n * n : (i + 1) * cols_per_thread * n;
        thread_args[i].matrix_x = matrix_x;
        thread_args[i].vector_y = vector_y;
        thread_args[i].n = n;
        thread_args[i].thread_id = i;
        thread_args[i].port = base_port + i + 1;
        thread_args[i].ip = ips[i];
        thread_args[i].core_id = i % max_cores;
        pthread_create(&threads[i], NULL, distribute_submatrices_to_slaves, (void *)&thread_args[i]);
    }

    for (int i = 0; i < num_slaves; i++){
        pthread_join(threads[i], NULL);
    }
}

void print_usage(char* program_name) {
    fprintf(stderr, "Usage for master: %s <matrix_size> <port_number> 0 <num_slaves> [--ip-file <filename> | slave_ip1 slave_ip2 ...]\n", program_name);
    fprintf(stderr, "Usage for slave:  %s <matrix_size> <port_number> 1 <num_slaves>\n", program_name);
}

int main(int argc, char *argv[]){
    struct timeval t1, t2;
    double time;
    
    int size, num_slaves, port, status, cols_per_thread;
    
    if (argc < 5) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    size = atoi(argv[1]);
    port = atoi(argv[2]);
    status = atoi(argv[3]);
    num_slaves = atoi(argv[4]);

    if (num_slaves <= 0 || num_slaves > MAX_THREADS) {
        fprintf(stderr, "Number of slaves must be between 1 and %d\n", MAX_THREADS);
        return EXIT_FAILURE;
    }
    
    cols_per_thread = size / num_slaves;
    if (size % num_slaves != 0) {
        fprintf(stderr, "Warning: Matrix size (%d) is not divisible by number of slaves (%d).\n", 
                size, num_slaves);
        fprintf(stderr, "Each slave will process %d columns, last slave will process extra columns.\n", 
                cols_per_thread);
    }
    
    int max_cores = sysconf(_SC_NPROCESSORS_ONLN) - 1;
    if (max_cores <= 0) max_cores = 1;
    
    printf("Using %d out of %d available cores\n", max_cores, max_cores + 1);

    if (status == 0){ 

        int* matrix_x = create_array(size);
        printf("Matrix X generated!\n");
        int* vector_y = create_vector(size);
        printf("Vector y generated!\n");
        int* vector_r = calloc(size, sizeof(int));

        char** slave_ips = (char**)malloc(num_slaves * sizeof(char*));
        int ip_count = 0;
        
        if (argc >= 7 && strcmp(argv[5], "--ip-file") == 0) {
            ip_count = read_slave_ips_from_file(argv[6], slave_ips, num_slaves);
            if (ip_count < 0) {
                fprintf(stderr, "Failed to read IP addresses from file: %s\n", argv[6]);
                free(slave_ips);
                return EXIT_FAILURE;
            }
            
            if (ip_count < num_slaves) {
                fprintf(stderr, "Warning: Only %d IP addresses found in file (expected %d).\n", 
                        ip_count, num_slaves);
                fprintf(stderr, "Proceeding with available addresses.\n");
                num_slaves = ip_count;
            }
        } 
        else if (argc >= 5 + num_slaves) {
            for (int i = 0; i < num_slaves; i++) {
                slave_ips[i] = (char*)malloc(MAX_IP_LENGTH * sizeof(char));
                if (slave_ips[i] == NULL) {
                    perror("Memory allocation failed for IP address");
                    
                    for (int j = 0; j < i; j++) {
                        free(slave_ips[j]);
                    }
                    free(slave_ips);
                    return EXIT_FAILURE;
                }
                
                strncpy(slave_ips[i], argv[5 + i], MAX_IP_LENGTH - 1);
                slave_ips[i][MAX_IP_LENGTH - 1] = '\0';
            }
            ip_count = num_slaves;
        } else {
            fprintf(stderr, "Master mode requires slave IP addresses via --ip-file or direct arguments\n");
            print_usage(argv[0]);
            free(slave_ips);
            return EXIT_FAILURE;
        }
        
        printf("Running in MASTER mode with %d slaves\n", num_slaves);
        for (int i = 0; i < num_slaves; i++) {
            printf("Slave %d: %s:%d\n", i+1, slave_ips[i], port+i+1);
        }

        gettimeofday(&t1, NULL);
        divide_matrix_to_slaves(matrix_x, size, cols_per_thread, num_slaves, port, slave_ips, max_cores, vector_y);
        gettimeofday(&t2, NULL);

        time = (double)(t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec) / 1000000.0;
        printf("Time to complete: %.8f seconds\n", time);

        for (int i = 0; i < num_slaves; i++) {
            free(slave_ips[i]);
        }
        free(slave_ips);
        free(vector_y);
        free_array(matrix_x);
        
    } else if (status == 1){
        printf("Running in SLAVE mode on port %d\n", port);
        
        int* submatrix = (int *)malloc(size * cols_per_thread * sizeof(int));
        int* vector_y = malloc(size * sizeof(int));
        if (submatrix == NULL){
            perror("Memory allocation failed");
            return EXIT_FAILURE;
        }

        receive_submatrix_from_master(port, size, cols_per_thread, submatrix, vector_y);
        printf("Submatrix received and processed!\n");
        
        free(vector_y);
        free(submatrix);
    } else {
        fprintf(stderr, "Invalid status. Use 0 for master, 1 for slave.\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}