#coding=utf-8
import curses
import time
scr = curses.initscr()
pad = curses.newpad(100, 100)
#  These loops fill the pad with letters; this is
# explained in the next section
curses.start_color()
curses.init_pair(1, curses.COLOR_RED, curses.COLOR_CYAN)

for i in xrange(20):
    scr.addstr(0, 0, "Current mode: Typing mode %s" % ('.'*i))
    scr.refresh()
    time.sleep(1)

