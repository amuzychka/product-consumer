#!/usr/bin/env python2
"""
Usefull links:
https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Condition_Objects_Producer_Consumer.php
"""

import argparse
import logging
import os
import random
import time
import signal
from threading import Thread, Condition, Event, Lock

msg_format = "%(asctime)s %(name)s %(levelname)-6s %(threadName)-10s|%(message)s"
dt_format = "%H:%M:%s"
logging.basicConfig(format=msg_format, datefmt=dt_format)

# Note: extra spaces in some log messages for better output visualization
logger = logging.getLogger("ProdCons")


class PositionLock(object):
    """
    Handle sharing consumers read position between threads. Each consumer should read data from
    new line, can work as context manager.

    position_lock = PositionLock()
    with position_lock() as plock:
        current_position = plock.position
        # ... do some stuff, change value for current_position
        plock.position = current_position
    """
    def __init__(self, start_position=0):
        """
        Initialize start_position and create lock
        :param start_position: start position for file reading
        """
        self.lock = Lock()
        self._position = start_position

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        if self.lock.locked():
            self.lock.release()

    @property
    def position(self):
        logger.debug("    now read: {} >>".format(self._position))
        return self._position

    @position.setter
    def position(self, value):
        logger.debug("    << next read: {}".format(value))
        self._position = value


class Producer(Thread):
    """Producer class: Write outputs to output file"""

    def __init__(self, output, condition, event, *args, **kwargs):
        """
        Producer instance
        :param output: str: path to the output file
        :param condition: threading.Condition: shared condition, notify consumers
        :param event: threading.Event: control exit condition
        :param args: args
        :param kwargs: kwargs
        """
        super(Producer, self).__init__(*args, **kwargs)
        self.output = output
        self.condition = condition
        self.exit = event

    def run(self):
        numbers = range(5)
        while not self.exit.is_set():
            with self.condition:
                number = random.choice(numbers)
                with open(self.output, 'a') as f:
                    logger.info("  writing \"{}\" at pos {}".format(number, f.tell()))
                    f.write(str(number))
                    f.write(os.linesep)
                    self.condition.notifyAll()
            time.sleep(random.random())


class Consumer(Thread):
    """Consumer class, read lines from input file"""

    def __init__(self, input, condition, event, position_lock, *args, **kwargs):
        """
        Consumer instance
        :param input: str: path to the input file
        :param condition: threading.Condition: shared condition, wait for producer
        :param event: threading.Event: control exit condition
        :param position_lock: PositionLock: position instance to sync consumer positions
        :param args: args
        :param kwargs: kwargs
        """
        super(Consumer, self).__init__(*args, **kwargs)
        self.input = input
        self.condition = condition
        self.exit = event
        self.position_lock = position_lock

    def run(self):
        while not self.exit.is_set():
            # FIXME: Ususally lock should be minimal in time.
            # I think it should be like:
            # * Wait for producer notification
            # * Try to lock with some timeout (because other consumer can acquire it)
            # * When lock is success - just read data, update position and unlock
            # * Process data

            # Done
            with self.condition:
                logger.info("      waiting for input..")
                start = time.time()
                self.condition.wait(0.1)
                logger.debug("      ..was waiting {} seconds".format(time.time() - start))
                with open(self.input, "r") as f:
                    with self.position_lock as plock:
                        current_read_position = plock.position
                        f.seek(0, os.SEEK_END)
                        last_file_position = f.tell()
                        if current_read_position != last_file_position:
                            f.seek(current_read_position)
                            line = f.readline().strip()
                            logger.info("reading  \"{}\" at pos {}".format(line, current_read_position))
                            plock.position = f.tell()
            time.sleep(random.random())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Run with debug verbosity")
    parser.add_argument("-f", "--file", dest="file_output",
                        default=os.path.join(os.path.dirname(__file__), "result"),
                        help="File where producer writes and consumer read. Default is "
                             "file location in the same place where script located")
    parser.add_argument("-a", "--follow", dest="follow", action="store_true", default=False,
                        help="Flag, if set start reading file from place where Producer started, "
                             "otherwise read file from begin.")

    args = parser.parse_args()

    logger.setLevel(level=logging.DEBUG if args.debug else logging.INFO)

    # Make sure that we can stop threads
    def keyboard_exit(signum, frame):
        logging.info("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(signum))
        raise

    signal.signal(signal.SIGTERM, keyboard_exit)
    signal.signal(signal.SIGINT, keyboard_exit)

    # Create empty file if it not exist, to prevent fail in case read before write
    if not os.path.exists(args.file_output):
        open(args.file_output, 'a').close()

    logging.info("Listening to file: {}".format(os.path.abspath(args.file_output)))

    start_position = 0
    if args.follow:
        with open(args.file_output, 'a') as f:
            start_position = f.tell()
            logging.info("Configured to read only new inputs..")
    logging.info("Starting from pos: {}".format(start_position))

    cond = Condition()
    event = Event()
    position = PositionLock(start_position)

    consumer1 = Consumer(args.file_output, cond, event,
                         position_lock=position, name="Consumer1")
    consumer2 = Consumer(args.file_output, cond, event,
                         position_lock=position, name="Consumer2")
    producer = Producer(args.file_output, cond, event=event, name="Producer")

    try:
        # FIXME: Try to improve functionality: one producer thread and 2 consumer threads (use Lock for lock start position for every consumer and also producer)
        # QUESTION: Why do we need to lock start position for producer?
        #           It always append data to the file, so start position == file.st_size

        consumer1.start()
        consumer2.start()
        producer.start()
        while True:
            time.sleep(0.5)
    except Exception:
        # FIXME: event object is the same for producer and consumers. I think event.set() is enought here. Right?
        # Right, thank you!
        event.set()
        consumer1.join()
        consumer2.join()
        producer.join()
