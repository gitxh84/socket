import re
import sys
import threading
import time

from client import Client
from constants import OutputWriter, BackupWriter
from server import Server


class Master(object):
    MAX_TIME_STOP_CLIENT_WAITS_FOR = 15     # seconds

    VALID_INPUTS = {
        "start_server": re.compile(r"start server (?P<addr>\d+\.\d+\.\d+\.\d+|localhost) (?P<port>\d+)"),
        "start_client": re.compile(r"start client (?P<index>\d) (?P<server_addr>\d+\.\d+\.\d+\.\d+|localhost) (?P<server_port>\d+)"),
        "stop_server": re.compile(r"stop server"),
        "stop_client": re.compile(r"stop client (?P<index>\d)"),
        "set_buffer": re.compile(r"(?P<index>\d) buffer (?P<size>\d+) (?P<num_recvs>\d+|-1)"),
        "client_msg": re.compile(r"(?P<index>\d) send (?P<num_characters>\d+)"),
        "wait": re.compile(r"wait (?P<seconds>\d+\.?\d*)")
    }

    def __init__(self, input_filepath):
        self.input_filepath = input_filepath

        self.server = None
        self.num_clients = 0
        self.clients = dict()

        # Record down the threads used to have the clients send messages.  This is needed for stopping a client.  We
        # need ot busy wait until the client has finished sending all of its messages (aka when all of these threads
        # are no longer alive).
        self.clients_sending_message_threads = {}

        self.read_input()

    def read_input(self):
        with open(self.input_filepath, "r") as input_file:
            for line in input_file:
                if len(line) == 0:
                    continue

                match = None
                for type, regex in Master.VALID_INPUTS.items():
                    match = re.match(regex, line)
                    if match and type == "start_server":
                        self.server = Server(match.group("addr"), int(match.group("port")), self.input_filepath)
                        self.server.start()
                        time.sleep(0.5)
                        break
                    elif match and type == "start_client":
                        assert self.server is not None, "Server has not been started, cannot start a client before server"
                        index = int(match.group("index"))
                        assert 0 <= index <= 9, "Index must be an integer between 0 and 9, inclusive"
                        if index not in self.clients_sending_message_threads:
                            self.clients_sending_message_threads[index] = []
                        self.clients[index] = Client(index, match.group("server_addr"), int(match.group("server_port")))
                        self.clients[index].start()
                        time.sleep(0.5)
                        break
                    elif match and type == "stop_server":
                        self.server.stop()
                        break
                    elif match and type == "stop_client":
                        index = int(match.group("index"))
                        client_sending_message_threads = self.clients_sending_message_threads[index]
                        max_time = Master.MAX_TIME_STOP_CLIENT_WAITS_FOR # seconds
                        while any([x.isAlive() for x in client_sending_message_threads]):
                            max_time -= 0.1
                            if max_time == 0:
                                raise Exception("Timeout on stop_client")
                            time.sleep(0.1)
                        self.clients[index].stop()
                        break
                    elif match and type == "set_buffer":
                        index = int(match.group("index"))
                        amt = int(match.group("size"))
                        num_messages = int(match.group("num_recvs"))
                        self.server.set_buffer_size(index, amt, num_messages)
                        break
                    elif match and type == "client_msg":
                        index = int(match.group("index"))
                        num_characters = int(match.group("num_characters"))
                        message = self.generate_message_1(index, num_characters, self.input_filepath)
                        assert index in self.clients, "Client {} has not been started, cannot send a message".format(index)
                        # Note that we need to create a thread here to run the client.send_message method or else this
                        # would block on client.send_message(message) until the message is fully sent
                        t = threading.Thread(target=self.clients[index].send_message, args=[message])
                        self.clients_sending_message_threads[index].append(t)
                        t.start()
                        break
                    elif match and type == "wait":
                        time.sleep(float(match.group("seconds")))
                        break

                if match is None:
                    raise Exception("'{}' is not a valid input in {}".format(line, self.input_filepath))

    def generate_message_0(self, index, num_characters, input_filepath):
        message = str(index) * num_characters
        file_to_backup_filepath = BackupWriter.gen_primary_filename(input_filepath, index)
        with open(file_to_backup_filepath, "w") as file_to_backup:
            file_to_backup.write(message)
        return message

    def generate_message_1(self, index, num_characters, input_filepath):
        with open("tests/12Commandments.txt", "rt") as text:
            message = text.read()[:num_characters]
        file_to_backup_filepath = BackupWriter.gen_primary_filename(input_filepath, index)
        with open(file_to_backup_filepath, "w") as file_to_backup:
            file_to_backup.write(message)
        return message

if __name__ == "__main__":
    # Supply the test .input file as the one and only argument (i.e. "python master.py tests/test.input")
    assert len(sys.argv) == 2, "master.py requires an argument for the *.input file to run on"
    input_filepath = sys.argv[1]
    assert input_filepath.endswith(".input"), "master.py must run on a *.input file"
    Master(input_filepath)
