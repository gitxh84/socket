import threading

from constants import RECEIVE_SIZE, MessageGenerator

# imports according to A3 instruction
import socket
import threading
import re
import time


class Client(threading.Thread):
    def __init__(self, index, server_addr, server_port):
        threading.Thread.__init__(self)

        self.clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.index = index
        self.address = server_addr
        self.port = server_port
        self.stopped = False


    def run(self):

        self.clientSocket.connect((self.address, self.port))

        # generate and send m_connection
        m_connection = MessageGenerator.gen_connection(self.index)
        self.clientSocket.send(m_connection.encode())

        # receive a message and make sure it is m_ready
        serverClosed, m_ready = self.get_message(self.clientSocket)
        assert(re.match(MessageGenerator.REGEX_MSG_READY, m_ready))

        # By Piazza @933, do not let thread die until stop() is called
        while (self.stopped == False):
            time.sleep(0.1)



    def stop(self):

        self.clientSocket.close()
        self.stopped = True



    def send_message(self, message):
        """
        Master calls this for the client to send a message to the server.
        :param message: String form of the message to send
        :return: None
        """

        total_count = len(message)

        # slow start
        size = 1
        sent_count = 0
        exponential = True

        while (sent_count < total_count):

            # generate and send m_data
            m_data = MessageGenerator.gen_data(message[:size])      # 'abc'[:7] = 'abc'
            self.clientSocket.send(m_data.encode())

            # receive a message and make sure it is m_response
            serverClosed, m_response = self.get_message(self.clientSocket)
            mat = re.match(MessageGenerator.REGEX_MSG_RESPONSE, m_response)
            assert(mat)

            # success
            if (mat.group('status') == 'S'):
                sent_count = sent_count + len(message[:size])
                message = message[size:]                                # 'abc'[7"] = ''

                if (exponential):
                    size = size * 2

                else:
                    size = size + 1

            else:
                size = size // 2
                exponential = False



    def get_message(self, the_socket):

        """
        Return (othersideClosed,message), where othersideClosed is a boolean
        evaluated to True if the other side (the server) has closed the
        connection, and message is a string representing a full message
        received on this side (the client). First we make sure to extract
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

        







        

