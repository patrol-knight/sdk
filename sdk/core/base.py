import time
import logging

from abc import ABC, abstractmethod


class BaseWorker(ABC):
    def __init__(
        self,
        name: str = "base",
        host: str = "localhost",
        port: int = 8000,
    ):
        self.host = host
        self.port = port

        logging.basicConfig(
            level=logging.INFO,
            format=(
                "[%(asctime)s] "
                "[%(name)s] "
                "[%(levelname)s] "
                "%(message)s"
            ),
        )
        self.logger = logging.getLogger(name)

        self.running: bool = False
        self.update_rate: float = 1.0

    def initialize(self) -> None:
        pass

    @abstractmethod
    def process(self) -> None:
        ...

    def run(self) -> None:
        self.running = True
        self.initialize()

        try:
            while self.running:
                try:
                    self.process()
                except Exception as exce:
                    self.logger.exception(exce)

                time.sleep(self.update_rate)

        except KeyboardInterrupt:
            self.stop() 

    def stop(self) -> None:
        if not self.run: return
        self.logger.info("Stopping")
        self.running = False
