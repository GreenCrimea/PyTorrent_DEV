from download import Run
from hypervisor import Hypervisor
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    id = 1

    download = Run()
    hypervisor = Hypervisor(download, id)
    id += 1
    hypervisor.start()