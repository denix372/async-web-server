// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;
static int efd;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memmove(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */

	if (strncmp(conn->request_path, "/static/", 8) == 0)
		return RESOURCE_TYPE_STATIC;
	if (strncmp(conn->request_path, "/dynamic/", 9) == 0)
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC) {
		sprintf(conn->send_buffer,
			"HTTP/1.1 200 OK\r\n"
			"Content-Length: %ld\r\n"
			"\r\n",
			conn->file_size);
		conn->send_len = strlen(conn->send_buffer);
		conn->send_pos = 0;
		conn->state = STATE_SENDING_DATA;
		return;
	}

	const char *msg = "HTTP/1.1 200 OK\r\n"
						"Content-Length: 5\r\n"
						"\r\n"
						"Hello";
	memmove(conn->send_buffer, msg, strlen(msg));
	conn->send_len = strlen(msg);
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	const char *msg = "HTTP/1.1 404 Not Found\r\n"
						"Content-Length: 0\r\n"
						"Connection: close\r\n"
						"\r\n";

	memmove(conn->send_buffer, msg, strlen(msg));
	conn->send_len = strlen(msg);
	conn->state = STATE_SENDING_DATA;
}

struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = calloc(1, sizeof(*conn));

	DIE(conn == NULL, "calloc");

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	if (connection_get_resource_type(conn) != RESOURCE_TYPE_DYNAMIC)
		return;

	conn->header_len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 200 OK\r\n"
		"Content-Length: %ld\r\n"
		"\r\n", conn->file_size);

	conn->send_len = conn->header_len;
	conn->send_pos = 0;

	size_t available_space = BUFSIZ - conn->header_len;
	size_t read_size = conn->file_size < available_space ? conn->file_size : available_space;

	memset(&conn->iocb, 0, sizeof(struct iocb));
	conn->piocb[0] = &conn->iocb;

	io_prep_pread(conn->piocb[0], conn->fd,
		conn->send_buffer + conn->header_len, read_size, 0);

	conn->piocb[0]->data = conn;
	io_set_eventfd(conn->piocb[0], efd);

	int rc = io_submit(ctx, 1, conn->piocb);

	DIE(rc < 0, "io_submit");
	conn->state = STATE_ASYNC_ONGOING;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	close(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	/* TODO: Set socket to be non-blocking. */
	int flags = fcntl(sockfd, F_GETFL, 0);

	DIE(flags < 0, "fcntl F_GETFL");

	rc = fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

	DIE(rc < 0, "fcntl F_SETFL");

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	/* TODO: Initialize HTTP_REQUEST parser. */
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	ssize_t bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len,
							  BUFSIZ - conn->recv_len, 0);

	if (bytes_recv <= 0) {
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		connection_remove(conn);
		return;
	}

	conn->recv_len += bytes_recv;
	conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	char path[512];

	strcpy(path, ".");
	strncat(path, conn->request_path, sizeof(path) - strlen(path) - 1);

	conn->fd = open(path, O_RDONLY);
	if (conn->fd < 0)
		return -1;

	struct stat st;

	fstat(conn->fd, &st);
	conn->file_size = st.st_size;
	conn->file_pos = 0;

	return 0;
}

static int aws_on_headers_complete_cb(http_parser *p)
{
	struct connection *conn = (struct connection *)p->data;

	conn->state = STATE_REQUEST_RECEIVED;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */

	/* Calculate how much was actually read */
	size_t available_space = BUFSIZ - conn->header_len;
	size_t read_size = conn->file_size < available_space ? conn->file_size : available_space;

	conn->send_len = conn->header_len + read_size;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;

	close(conn->fd);
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	http_parser parser;

	http_parser_init(&parser, HTTP_REQUEST);
	parser.data = conn;
	http_parser_settings settings = {0};

	settings.on_path = aws_on_path_cb;
	settings.on_headers_complete = aws_on_headers_complete_cb;

	size_t parsed = http_parser_execute(&parser, &settings, conn->recv_buffer, conn->recv_len);

	if (parsed == conn->recv_len)
		return 0;
	return -1;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	ssize_t sent = sendfile(conn->sockfd, conn->fd, (off_t *)&conn->file_pos,
						conn->file_size - conn->file_pos);

	if (sent < 0)
		return STATE_CONNECTION_CLOSED;

	if (conn->file_pos == conn->file_size)
		return STATE_DATA_SENT;

	return STATE_SENDING_DATA;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t sent = send(conn->sockfd, conn->send_buffer + conn->send_pos,
						conn->send_len - conn->send_pos, 0);
	if (sent < 0)
		return -1;

	conn->send_pos += sent;

	if (conn->send_pos == conn->send_len)
		return 1;
	return 0;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	int rc = connection_send_data(conn);
	if (rc < 0)
		return -1;
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	receive_data(conn);
	parse_header(conn);
	if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC) {
		if (connection_open_file(conn) < 0)
			connection_prepare_send_404(conn);
		else
			connection_prepare_send_reply_header(conn);

		w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
		return;

	} else if (connection_get_resource_type(conn) == RESOURCE_TYPE_DYNAMIC) {
		if (connection_open_file(conn) < 0) {
			connection_prepare_send_404(conn);
			w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
			return;
		}
		connection_start_async_io(conn);
		return;
	}

	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC) {
		if (conn->send_pos < conn->send_len) {
			int done = connection_send_data(conn);

			if (done < 0) {
				w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
				connection_remove(conn);
				return;
			}

			if (done == 0)
				return;
		}

		enum connection_state st = connection_send_static(conn);

		if (st == STATE_DATA_SENT || st == STATE_CONNECTION_CLOSED) {
			w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			connection_remove(conn);
		}
		return;
	}

	/* For dynamic resources, only send if we're in the sending state */
	if (conn->state != STATE_SENDING_DATA)
		return;

	int rc = connection_send_data(conn);
	if (rc < 0) {
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		connection_remove(conn);
		return;
	}

	if (rc == 1) {
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		connection_remove(conn);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
}

int main(void)
{
	int rc;
	// analizand fisierul epoll_echo_server.c din folderul samples
	//ne dam seama cum sa initilizam serverul

	/* TODO: Initialize asynchronous operations. */
	memset(&ctx, 0, sizeof(ctx));
	rc = io_setup(128, &ctx);
	DIE(rc < 0, "io_setup");

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	efd = eventfd(0, EFD_NONBLOCK);
	DIE(efd < 0, "eventfd");

	rc = w_epoll_add_fd_in(epollfd, efd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */

		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else if (rev.data.fd == efd) {
			unsigned long int cnt;
			ssize_t r = read(efd, &cnt, sizeof(cnt));
			if (r < 0 && errno != EAGAIN)
				DIE(1, "read eventfd");

			struct io_event events[128];
			int nevents = io_getevents(ctx, 1, 128, events, NULL);
			DIE(nevents < 0, "io_getevents");

			for (int i = 0; i < nevents; i++) {
				struct connection *conn = events[i].data;
				connection_complete_async_io(conn);
			}
		} else {
			struct connection *conn = rev.data.ptr;

			if (rev.events & EPOLLIN)
				handle_input(conn);

			if (rev.events & EPOLLOUT)
				handle_output(conn);
		}
	}

	return 0;
}
