#!/usr/bin/env python2
import argparse
import logging
import os
import random
import time
import signal

from threading import Thread, Condition, Event


_START_POSITION = 0  # global usage


class Producer(Thread):
    """Producer class: Write outputs to output file"""

    def __init__(self, output, *args, **kwargs):
        """
        Producer instance
        :param output: str: path to the output file
        :param args: args
        :param kwargs: kwargs
        """
        super(Producer, self).__init__(name=self.__class__.__name__,
                                       args=args, kwargs=kwargs)
        self.output = output
        # FIXME: exit event should be one for all entities/classes and coming in parameters
        self.exit = Event()

    def run(self):
        logger = logging.getLogger(self.__class__.__name__)
        numbers = range(5)
        while not self.exit.is_set():
            # FIXME: Condition also must be as an input parameter in __init__
            with condition:
                number = random.choice(numbers)
                with open(self.output, 'a') as f:
                    logger.info("writing \"{}\" at pos {}".format(number, f.tell()))
                    # FIXME: Use os.linesep instead of '\n'
                    f.write("{}\n".format(number))
                # FIXME: I think notifyAll() should be here (when, for example, you have more than one consumer thread)
                # See https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Condition_Objects_Producer_Consumer.php
                condition.notify()
            time.sleep(random.random())


class Consumer(Thread):
    """Consumer class, read lines from input file"""

    def __init__(self, input, follow=False, *args, **kwargs):
        """
        Consumer instance
        :param input: str: path to the input file
        :param follow: boolean: if True - takes as start position value from
                                variable "_START_POSITION"
        :param args: args
        :param kwargs: kwargs
        """
        super(Consumer, self).__init__(name=self.__class__.__name__,
                                       args=args, kwargs=kwargs)
        self.input = input
        self.follow = follow
        self.exit = Event()

    def run(self):
        logger = logging.getLogger(self.__class__.__name__)
        global _START_POSITION
        self.position = _START_POSITION if self.follow else 0

        while not self.exit.is_set():
            with condition:
                with open(self.input, "r") as f:
                    f.seek(self.position)
                    line = f.readline().strip()
                    if self.position == f.tell():
                        logger.info("waiting for input..")
                        start = time.time()
                        condition.wait()
                        logger.debug("  was waiting {} seconds".format(time.time() - start))
                        line = f.readline().strip()
                    logger.info("  reading  \"{}\" at pos {}".format(line, self.position))
                    self.position = f.tell()
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
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%H:%M:%s')
    # Create empty file if it not exist, to prevent fail in case read before write
    if not os.path.exists(args.file_output):
        open(args.file_output, 'a').close()

    logging.info("Listening to file: {}".format(os.path.abspath(args.file_output)))
    if args.follow:
        with open(args.file_output, 'a') as f:
            _START_POSITION = f.tell()
            logging.info(
                "Configured to read only new inputs, started from pos: {}".format(_START_POSITION))


    def keyboard_exit(signum, frame):
        logging.info("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(signum))
        raise

    signal.signal(signal.SIGTERM, keyboard_exit)
    signal.signal(signal.SIGINT, keyboard_exit)

    condition = Condition()
    consumer = Consumer(input=args.file_output, follow=args.follow)
    producer = Producer(output=args.file_output)

    try:
        # FIXME: Try to improve functionality: one producer thread and 2 consumer threads (use Lock for lock start position for every consumer and also producer)
        consumer.start()
        producer.start()
        while True:
            time.sleep(0.5)
    except Exception:
        consumer.exit.set()
        producer.exit.set()
        consumer.join()
        producer.join()
