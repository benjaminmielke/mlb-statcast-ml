#!/usr/bin/env python
# coding: utf-8

from mlb_statcast import Statcast_DB

statcast = Statcast_DB()

statcast.stream_data('2019-09-29')
