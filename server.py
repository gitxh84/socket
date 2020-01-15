import threading

from constants import RECEIVE_SIZE, MessageGenerator, OutputWriter, BackupWriter

# imports according to A3 instruction and Piazza posts
import socket
import threading
import re
import time
import select
from collections import deque



class Server(threading.Thread):
    def __init__(self, addr, port, input_filename):
        threading.Thread.__init__(self)
        
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.address = addr
        self.port = port
        self.input_filename = input_filename
        self.output = OutputWriter(OutputWriter.gen_output_filename(input_filename))
        self.stopped = False

        # [deque[(2,3),(0,1)],...] means client 0 has 3 trials of buffer size 2 and 1 trial of buffer size 0
        self.buffer = []
        for i in range(10):
            self.buffer.append(deque([]))

        self.buffer_forever = [False]*10    # True if a foreve buffer size has been set
        self.buffer_firsttime = [True]*10   # True if first time setting buffer



    def run(self):

        self.serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSocket.bind((self.address, self.port))
        self.serverSocket.listen(1)
        self.serverSocket.setblocking(0)

        epoll = select.epoll()
        epoll.register(self.serverSocket.fileno(), select.EPOLLIN)


        try:

            ready_list = {}                 # maps file descriptors to m_ready
            success_list = {}               # maps file descriptors to success m_response
            fail_list = {}                  # maps file descriptors to fail m_response

            connections = {}                # maps file descriptors to socket
            requests = {}                   # maps file descriptors to received msg

            m_connection_received = {}      # maps file descriptors to boolean
            m_data_received ={}             # maps file descriptors to boolean

            backups = {}                    # map file descriptors to backup object
            indices = {}                    # map file descriptors to client index
            exceeded = {}                   # map file descriptors to bolean


            while (self.stopped == False or len(connections) > 0):

                events = epoll.poll(0.2)
                for fileno, event in events:

                    # (serverSocket,read)
                    if (self.stopped == False and fileno == self.serverSocket.fileno()):

                        connectionSocket, addr = self.serverSocket.accept()
                        connectionSocket.setblocking(0)
                        epoll.register(connectionSocket.fileno(), select.EPOLLIN)
                        
                        ready_list[connectionSocket.fileno()] = MessageGenerator.gen_ready()
                        success_list[connectionSocket.fileno()] = MessageGenerator.gen_response(True)
                        fail_list[connectionSocket.fileno()] = MessageGenerator.gen_response(False)

                        connections[connectionSocket.fileno()] = connectionSocket
                        requests[connectionSocket.fileno()] = ''

                        m_connection_received[connectionSocket.fileno()] = False
                        m_data_received[connectionSocket.fileno()] = False


                    # (clientSocket,read)
                    elif (event & select.EPOLLIN):

                        chunk = connections[fileno].recv(RECEIVE_SIZE).decode()

                        # client closed
                        if (len(chunk) == 0):

                            epoll.unregister(fileno)
                            connections[fileno].close()
                            self.output.write_connection_end(indices[fileno])

                            del ready_list[fileno]
                            del success_list[fileno]
                            del fail_list[fileno]

                            del connections[fileno]
                            del requests[fileno]

                            del m_connection_received[fileno]
                            del m_data_received[fileno]

                            if (fileno in backups):
                                backups[fileno].close()
                                del backups[fileno]

                            if (fileno in indices):
                                del indices[fileno]

                            if (fileno in exceeded):
                                del exceeded[fileno]


                        # client did not close
                        else:

                            requests[fileno] = requests[fileno] + chunk
                            mat = re.match(MessageGenerator.REGEX_VALID_TCP_BUFFER, requests[fileno])

                            if (mat is not None):
                                length = int(mat.group('payload_length'))
                                message = mat.group('payload')

                                # received full message
                                if (len(message) == length):

                                    epoll.modify(fileno, select.EPOLLOUT)

                                    # m_connection has not been received
                                    if (m_connection_received[fileno] == False):

                                        # make sure this is m_connection
                                        mat = re.match(MessageGenerator.REGEX_MSG_CONNECTION, message)
                                        assert(mat)

                                        indices[fileno] = int(mat.group('index'))
                                        m_connection_received[fileno] = True
                                        requests[fileno] = ''


                                    # m_connection has been received
                                    else:

                                        # make sure this is m_data
                                        mat = re.match(MessageGenerator.REGEX_MSG_DATA, message)
                                        assert(mat)
                                        m_data = mat.group('data')

                                        m_data_received[fileno] = True
                                        requests[fileno] = ''

                                        # determine buffer size
                                        tup = self.buffer[indices[fileno]].popleft()
                                        size = tup[0]
                                        recvs = tup[1]

                                        # exceed buffer size
                                        if (len(m_data) > size):

                                            exceeded[fileno] = True
                                            self.output.write_received(indices[fileno], str(m_data), False)


                                        # did not exceed buffer size
                                        else:

                                            exceeded[fileno] = False
                                            backups[fileno].append_to_backup_file(m_data)
                                            self.output.write_received(indices[fileno], str(m_data), True)


                                        # update buffer
                                        if (recvs == 1):

                                            # By instruction, "return to 0 if there are no more buffer-setting commands remaining"
                                            if (len(self.buffer[indices[fileno]]) == 0):
                                                self.set_buffer_size(indices[fileno], 0, 3)

                                            else:
                                                # generate output
                                                new_size = self.buffer[indices[fileno]][0][0]
                                                self.output.write_buffer_set(indices[fileno], new_size)

                                        elif (recvs == -1):
                                            self.buffer[indices[fileno]].appendleft((size,recvs))

                                        # By Piazza @809, num_recvs can take on a value of -1 or >= 1, cannot be 0
                                        else:
                                            self.buffer[indices[fileno]].appendleft((size,recvs-1))


                    # (clientSocket,write)
                    elif (event & select.EPOLLOUT):

                        # should send m_ready
                        if (m_connection_received[fileno] == True and m_data_received[fileno] == False):

                            connections[fileno].send(ready_list[fileno].encode())
                            epoll.modify(fileno, select.EPOLLIN)

                            # output connection starts and set up backup writer
                            self.output.write_connection_start(indices[fileno])
                            backups[fileno] = BackupWriter(BackupWriter.gen_backup_filename(self.input_filename, indices[fileno]))


                        # shoud send m_response
                        elif (m_connection_received[fileno] == True and m_data_received[fileno] == True):

                            if (exceeded[fileno] == True):

                                connections[fileno].send(fail_list[fileno].encode())
                                epoll.modify(fileno, select.EPOLLIN)
                                m_data_received[fileno] = False

                            else:
                                
                                connections[fileno].send(success_list[fileno].encode())
                                epoll.modify(fileno, select.EPOLLIN)
                                m_data_received[fileno] = False


                    # (clientSocket,closed)
                    elif (event & select.EPOLLHUP):

                        # Piazza @1192, do this to be safe
                        if (fileno in connections):

                            epoll.unregister(fileno)
                            connections[fileno].close()
                            self.output.write_connection_end(indices[fileno])

                            del ready_list[fileno]
                            del success_list[fileno]
                            del fail_list[fileno]

                            del connections[fileno]
                            del requests[fileno]

                            del m_connection_received[fileno]
                            del m_data_received[fileno]

                            if (fileno in backups):
                                backups[fileno].close()
                                del backups[fileno]

                            if (fileno in indices):
                                del indices[fileno]

                            if (fileno in exceeded):
                                del exceeded[fileno]

                            
        finally:
            
            if (self.stopped == False):
                epoll.unregister(self.serverSocket.fileno())
            
            epoll.close()


        self.serverSocket.close()       # moved from stop() to here after OH
        self.output.close()  



    def stop(self):

        self.stopped = True




    def set_buffer_size(self, index, buffer_size, num_recvs):
        """
        Master will call this method to set the buffer size for a certain index.
        :param index: Integer index of the a certain client
        :param buffer_size: Integer size of buffer.
        :param num_recvs: Integer number of receives the server stays at this buffer size for; -1 = forever
        :return: None, By Piazza @1056, return None means no return
        """

        #print('original buffer is ')
        #print(self.buffer)

        # first call
        if (self.buffer_firsttime[index] == True):
            self.output.write_buffer_set(index, buffer_size)
            self.buffer_firsttime[index] = False

        # if -1 then "any subsequent buffer size setting would then be ignored"
        if (self.buffer_forever[index] == False):

            # forever this buffer size
            if (num_recvs == -1):

                self.buffer_forever[index] = True
            
            self.buffer[index].append((buffer_size,num_recvs))




    def get_message(self, the_socket):

        """
        Return (othersideClosed,message), where othersideClosed is a boolean
        evaluated to True if the other side (the client) has closed the
        connection, and message is a string representing a full message
        received on this side (the server). First we make sure to extract
        <messsage_size>, then keep receiving message until the accumulated
        message reaches <message_size>. Meanwhile, if an empty string is
        received then it means the other side has closed connection. 
        
        Examples: (<messsage_size> is # letters, not # bytes for simplicity)
        "12:CONNECTION|2", did not closed ==> (False,"CONNECTION|2")
        "11:DATA|thanks", closed after ==> (True,"DATA|thanks")

        Parameters:
        the_socket: the socket through which we try to receive message.

        """

        received = ''
        mat = None
        while ((mat is None)):

            chunk = the_socket.recv(RECEIVE_SIZE).decode()

            if (len(chunk) == 0):
                return (True,'')

            received = received + chunk
            mat = re.match(MessageGenerator.REGEX_VALID_TCP_BUFFER, received)

        length = int(mat.group('payload_length'))
        message = mat.group('payload')

        while (len(message) < length):

            chunk = the_socket.recv(RECEIVE_SIZE).decode()

            if (len(chunk) == 0):
                return (True,message) 

            message = message + chunk

        return (False,message)



    @staticmethod
    def extract_message(tcp_buffer):
        """
        IMPLEMENTATION OF THIS IS TOTALLY OPTIONAL, BUT YOU MIGHT FIND THIS FUNCTION USEFUL.  Read from the buffer and
        determine whether or not the full message was received (this is needed because TCP is a stream protocol).
        Returns a 2-tuple as described below.
        :param tcp_buffer: Buffer for TCP receives containing the data sent from client
        :return: (message if fully received else None, tcp_buffer without the extracted message)
        """
        pass
