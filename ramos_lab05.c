#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <float.h>
#include <limits.h>
#include <sched.h>

// Common defines
#define MAX_MATRIX_SIZE 30000
#define CHUNK_SIZE 1000
#define MAX_CLIENTS 100
#define MAX_IP_LEN 16
#define CONFIG_FILE "config.txt"

// Function to get time in seconds with microsecond precision
double get_time_s() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

// Memory allocation functions
int **allocate_matrix(int rows, int cols) {
    int **matrix = (int **)malloc(rows * sizeof(int *));
    if (!matrix) {
        perror("Matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i++) {
        matrix[i] = (int *)malloc(cols * sizeof(int));
        if (!matrix[i]) {
            perror("Matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }
    return matrix;
}

float **allocate_float_matrix(int rows, int cols) {
    float **matrix = (float **)malloc(rows * sizeof(float *));
    if (!matrix) {
        perror("Float matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < rows; i++) {
        matrix[i] = (float *)malloc(cols * sizeof(float));
        if (!matrix[i]) {
            perror("Float matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }
    return matrix;
}

void free_matrix(int **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
}

void free_float_matrix(float **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
}

// Matrix creation function
int **create_random_matrix(int rows, int cols) {
    int **matrix = allocate_matrix(rows, cols);
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            matrix[i][j] = rand() % 100;  // Random numbers between 0-99
        }
    }
    return matrix;
}

// Client-server communication functions
void send_submatrix(int sock, int **matrix, int start_row, int end_row, int cols) {
    int rows = end_row - start_row;
    int dimensions[2] = {rows, cols};
    if (send(sock, dimensions, sizeof(dimensions), 0) < 0) {
        perror("Send dimensions failed");
        exit(EXIT_FAILURE);
    }

    for (int chunk_start = 0; chunk_start < rows; chunk_start += CHUNK_SIZE) {
        int chunk_end = (chunk_start + CHUNK_SIZE < rows) ? chunk_start + CHUNK_SIZE : rows;
        int chunk_rows = chunk_end - chunk_start;

        if (send(sock, &chunk_rows, sizeof(int), 0) < 0) {
            perror("Send chunk rows failed");
            exit(EXIT_FAILURE);
        }

        for (int i = chunk_start; i < chunk_end; i++) {
            if (send(sock, matrix[start_row + i], cols * sizeof(int), 0) < 0) {
                perror("Send row failed");
                exit(EXIT_FAILURE);
            }
        }
    }
}

int **receive_matrix(int sock, int *rows, int *cols) {
    int dimensions[2];
    if (recv(sock, dimensions, sizeof(dimensions), MSG_WAITALL) != sizeof(dimensions)) {
        perror("Receive dimensions failed");
        exit(EXIT_FAILURE);
    }
    *rows = dimensions[0];
    *cols = dimensions[1];

    int **matrix = allocate_matrix(*rows, *cols);
    int received_rows = 0;
    while (received_rows < *rows) {
        int chunk_rows;
        if (recv(sock, &chunk_rows, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("Receive chunk rows failed");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < chunk_rows; i++) {
            if (recv(sock, matrix[received_rows + i], *cols * sizeof(int), MSG_WAITALL) != *cols * sizeof(int)) {
                perror("Receive row failed");
                exit(EXIT_FAILURE);
            }
        }
        received_rows += chunk_rows;
    }

    return matrix;
}

// Server-specific data structures and functions
typedef struct {
    char ip[MAX_IP_LEN];
    int port;
    int socket;
    int start_row;
    int end_row;
    float **partial_result;
    int rows;
    int cols;
} ClientInfo;

// Global variables for matrix distribution (server mode only)
int **global_matrix;
int global_rows, global_cols;
ClientInfo *clients;
int client_count = 0;

int read_client_config() {
    FILE *config = fopen(CONFIG_FILE, "r");
    if (config == NULL) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    char ip[MAX_IP_LEN];
    int port;
    int count = 0;

    while (fscanf(config, "%s %d", ip, &port) == 2) {
        count++;
    }

    if (count == 0) {
        printf("No clients found in config file\n");
        fclose(config);
        exit(EXIT_FAILURE);
    }

    clients = (ClientInfo *)malloc(count * sizeof(ClientInfo));
    if (!clients) {
        perror("Failed to allocate memory for clients");
        fclose(config);
        exit(EXIT_FAILURE);
    }

    rewind(config);
    client_count = 0;
    while (fscanf(config, "%s %d", ip, &port) == 2 && client_count < count) {
        strncpy(clients[client_count].ip, ip, MAX_IP_LEN - 1);
        clients[client_count].port = port;
        clients[client_count].socket = -1;
        client_count++;
    }

    fclose(config);
    printf("Read %d clients from config file\n", client_count);
    return client_count;
}

void connect_to_clients() {
    struct sockaddr_in address;
    address.sin_family = AF_INET;

    for (int i = 0; i < client_count; i++) {
        if ((clients[i].socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        address.sin_port = htons(clients[i].port);
        if (inet_pton(AF_INET, clients[i].ip, &address.sin_addr) <= 0) {
            printf("Invalid address for client %d: %s\n", i, clients[i].ip);
            continue;
        }

        printf("Connecting to client %d at %s:%d...\n", i, clients[i].ip, clients[i].port);
        if (connect(clients[i].socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
            printf("Failed to connect to client %d at %s:%d\n", i, clients[i].ip, clients[i].port);
            close(clients[i].socket);
            clients[i].socket = -1;
            continue;
        }

        printf("Connected to client %d at %s:%d\n", i, clients[i].ip, clients[i].port);
    }
}

void distribute_matrix_work() {
    int active_clients = 0;
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            active_clients++;
        }
    }

    if (active_clients == 0) {
        printf("No active clients to distribute work to\n");
        exit(EXIT_FAILURE);
    }

    int rows_per_client = global_rows / active_clients;
    int extra_rows = global_rows % active_clients;

    int current_row = 0;
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            clients[i].start_row = current_row;

            int client_rows = rows_per_client;
            if (extra_rows > 0) {
                client_rows++;
                extra_rows--;
            }

            clients[i].end_row = current_row + client_rows;
            current_row += client_rows;

            clients[i].rows = client_rows;
            clients[i].cols = global_cols;

            printf("Client %d assigned rows %d to %d\n",
                   i, clients[i].start_row, clients[i].end_row - 1);
        }
    }
}

void run_server(int matrix_size) {
    srand(time(NULL));

    printf("Reading client configuration from %s...\n", CONFIG_FILE);
    read_client_config();

    global_rows = matrix_size;
    global_cols = matrix_size;
    printf("Creating %dx%d matrix X and vector y...\n", global_rows, global_cols);
    global_matrix = create_random_matrix(global_rows, global_cols);
    float *vector_y = (float *)malloc(global_rows * sizeof(float));
    for (int i = 0; i < global_rows; i++) {
        vector_y[i] = rand() % 100;
    }

    double time_before = get_time_s();
    printf("Starting matrix distribution at %.2f s\n", time_before);

    printf("Connecting to clients...\n");
    connect_to_clients();

    printf("Distributing matrix X and vector y to clients...\n");
    distribute_matrix_work();

    float *vector_e = (float *)malloc(global_cols * sizeof(float));
    memset(vector_e, 0, global_cols * sizeof(float));
    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            float *partial_e = (float *)malloc(clients[i].cols * sizeof(float));
            if (recv(clients[i].socket, partial_e, clients[i].cols * sizeof(float), MSG_WAITALL) != clients[i].cols * sizeof(float)) {
                perror("Failed to receive part of vector e");
                exit(EXIT_FAILURE);
            }
            memcpy(vector_e + clients[i].start_row, partial_e, clients[i].cols * sizeof(float));
            free(partial_e);
        }
    }

    double time_after = get_time_s();
    double time_elapsed = time_after - time_before;
    printf("\nTotal processing time: %.2f s\n", time_elapsed);

    printf("Sample of vector e (up to 10 elements):\n");
    for (int i = 0; i < (global_cols < 10 ? global_cols : 10); i++) {
        printf("%.4f ", vector_e[i]);
    }
    printf("\n");

    free_matrix(global_matrix, global_rows);
    free(vector_y);
    free(vector_e);

    for (int i = 0; i < client_count; i++) {
        if (clients[i].socket != -1) {
            close(clients[i].socket);
        }
    }
    free(clients);

    printf("Process completed successfully\n");
}

void run_client(int port) {
    int server_fd, client_sock;
    struct sockaddr_in address, client_addr;
    int opt = 1;
    int addrlen = sizeof(client_addr);
    int client_port = port;

    printf("Starting client on port %d\n", client_port);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(client_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 1) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Client listening on port %d for server connection...\n", client_port);

    if ((client_sock = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t*)&addrlen)) < 0) {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    printf("Server connected\n");

    int rows, cols, y_size;
    printf("Waiting to receive matrix X and vector y from server...\n");
    int **matrix_X = receive_matrix(client_sock, &rows, &cols);
    float *vector_y = (float *)malloc(y_size * sizeof(float));
    if (recv(client_sock, vector_y, y_size * sizeof(float), MSG_WAITALL) != y_size * sizeof(float)) {
        perror("Failed to receive vector y");
        exit(EXIT_FAILURE);
    }
    printf("Received %dx%d submatrix X and vector y of size %d from server\n", rows, cols, y_size);

    double time_before = get_time_s();

    float *vector_e = (float *)malloc(cols * sizeof(float));
    for (int j = 0; j < cols; j++) {
        float mse = 0.0f;
        for (int i = 0; i < rows; i++) {
            float diff = matrix_X[i][j] - vector_y[i];
            mse += diff * diff;
        }
        vector_e[j] = mse / rows;
    }

    double time_after = get_time_s();
    double time_elapsed = time_after - time_before;
    printf("MSE computation completed in %.6f seconds\n", time_elapsed);

    printf("Sending computed vector e back to server...\n");
    if (send(client_sock, vector_e, cols * sizeof(float), 0) < 0) {
        perror("Failed to send vector e");
        exit(EXIT_FAILURE);
    }
    printf("Vector e sent back to server\n");

    free_matrix(matrix_X, rows);
    free(vector_y);
    free(vector_e);
    close(client_sock);
    close(server_fd);

    printf("Client process completed successfully\n");
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        printf("Usage: %s <matrix_size> <port> <0=server|1=client>\n", argv[0]);
        return 1;
    }

    int matrix_size = atoi(argv[1]);
    int port = atoi(argv[2]);
    int is_client = atoi(argv[3]);

    if (matrix_size <= 0) {
        printf("Matrix size must be positive\n");
        return 1;
    }

    if (port <= 0) {
        printf("Port must be positive\n");
        return 1;
    }

    if (is_client != 0 && is_client != 1) {
        printf("Mode must be 0 (server) or 1 (client)\n");
        return 1;
    }

    printf("Starting %s mode with matrix size %d on port %d\n",
           is_client ? "CLIENT" : "SERVER", matrix_size, port);

    if (is_client) {
        run_client(port);
    } else {
        run_server(matrix_size);
    }

    return 0;
}