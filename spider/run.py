# _*_coding:utf-8_*_
# Author by Mr.Xu

import io
import sys
from spider.spiders.scheduler import Scheduler

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def main():
    try:
        s = Scheduler()
        s.run()
    except:
        main()


if __name__ == '__main__':
    main()
