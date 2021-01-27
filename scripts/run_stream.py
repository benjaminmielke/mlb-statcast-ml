#!/usr/bin/env python
# coding: utf-8

from mlb_statcast import Statcast_DB

statcast = Statcast_DB()

statcast.run_stream('2019-06-03')
