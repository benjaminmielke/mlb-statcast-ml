#!/usr/bin/env python
# coding: utf-8

from mlb_statcast import Statcast_DB

statcast = Statcast_DB()

statcast.build_db()

# import pos_bop_stream
#
# pos_bop_stream.stream_pos_bop_dct('2020-09-27')
