import logging
import os

from constant import State
from rb_extractor import RbExtractor
import threading
import argparse

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def run():
    extractors = [
        RbExtractor(0, State.BADEN_WUETTEMBERG),
        RbExtractor(0, State.BAYERN),
        RbExtractor(0, State.BERLIN),
        RbExtractor(0, State.BRANDENBURG),
        RbExtractor(0, State.BREMEN),
        RbExtractor(0, State.HAMBURG),
        RbExtractor(0, State.HESSEN),
        RbExtractor(0, State.MECKLENBURG_VORPOMMERN),
        RbExtractor(0, State.NIEDERSACHSEN),
        RbExtractor(0, State.NORDRHEIN_WESTFALEN),
        RbExtractor(0, State.RHEILAND_PFALZ),
        RbExtractor(0, State.SAARLAND),
        RbExtractor(0, State.SACHSEN),
        RbExtractor(0, State.SACHSEN_ANHALT),
        RbExtractor(7831, State.SCHLESWIG_HOLSTEIN),
        RbExtractor(0, State.THUERINGEN),
            ]
    threads = []

    threads = [threading.Thread(target=extractor.extract) for extractor in extractors]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    run()
