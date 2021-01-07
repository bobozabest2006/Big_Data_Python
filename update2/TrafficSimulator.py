import os
import time


class TrafficGenerator(object):
    # Set PCAP Record length
    len_pcap_rec = 16
    # Set PVS Header length
    len_pvs_header = 12
    # Set SPW Header length
    len_spw_header = 12
    # Class object counter
    count = 0
    # Store read bytes for decoding
    read_bytes = None

    def __init__(self, pcap_path):
        # Class object counter
        TrafficGenerator.count += 1

        # Open file for parsing
        self.pcap_file = open(pcap_path, mode='rb')
        # Set reference point at the beginning of the first packet
        self.pcap_file.seek(24, 0)
        # Get file size
        self.pcap_size = os.path.getsize(pcap_path)

    def generator(self):
        # Remember cursor last position
        prev_position = self.pcap_file.tell()

        # Read packet entire header (pcap header + pvs header + spw header)
        self.read_bytes = self.pcap_file.read(self.len_pcap_rec +
                                              self.len_pvs_header +
                                              self.len_spw_header)

        # Convert incl_len bytes to integer
        pcap_record_incl_len = int.from_bytes(self.read_bytes[28:32], byteorder='little')
        # # Convert orig_len bytes to integer
        # pcap_record_orig_len = int.from_bytes(self.read_bytes[12:16], byteorder='little')

        # Move file cursor to the start of the packet
        self.pcap_file.seek(prev_position)

        # Packet length on wire
        pkt_length = pcap_record_incl_len + self.len_pcap_rec + self.len_pvs_header + self.len_spw_header

        # Read packet
        packet_bytes = self.pcap_file.read(pkt_length)

        return packet_bytes

    # Class iterator
    def __iter__(self):
        # Initialise iterator index
        self.iter_position = 1

        return self

    def __next__(self):
        # Packet key-value pairs
        values = self.generator()

        # End of file check
        if not self.read_bytes:
            raise StopIteration
            # self.pcap_file.seek(24, 0)

        # Increment index for class iterator to decode next packet
        self.iter_position += 1

        return values

    def __del__(self):
        TrafficGenerator.count -= 1

        if TrafficGenerator.count == 0:
            self.pcap_file.close()


def main():
    # Start execution time
    time_start = time.time()

    # Path to pcap file
    pcap_file = '/home/cloudera-master/2018-04-05_09-36-26_Default_SPW-4.pcap'

    # Test purposes only
    pkt_d = enumerate(TrafficGenerator(pcap_file))
    # Packet 1 parameters
    for i in range(10):
        print(type(next(pkt_d)[0]))
        print(next(pkt_d))

    # End execution time
    time_end = time.time()

    # Runtime in seconds
    print('Process finished in {} seconds'.format(time_end - time_start))


if __name__ == '__main__':
    main()
