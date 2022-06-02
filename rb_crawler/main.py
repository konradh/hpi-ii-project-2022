import logging
import os

import click

from constant import State
from rb_extractor import RbExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
@click.option("-i", "--id", "rb_id", type=int, help="The rb_id to initialize the crawl from")
@click.option("-s", "--state", type=click.Choice(State), help="The state ISO code")
@click.option("-d", "--delay", type=float, help="The delay between each request")
def run(rb_id: int, state: State, delay: float):
    RbExtractor(rb_id, state, delay=delay).extract()


if __name__ == "__main__":
    run()
