#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 12:53:45 2018

@author: angli
"""

from revscoring.extractors import api
import mwapi
from revscoring.features import wikitext

extractor = api.Extractor(mwapi.Session("https://en.wikipedia.org", user_agent="Revscoring feature ... "))
list(extractor.extract(639813656, [wikitext.revision.diff.datasources.segments_added, wikitext.revision.diff.datasources.segments_removed]))
#[['a protest, that had been using ', 'slogan,'], [',', 'movement', ' during protests']]


import mwpersistence
import deltas

state = mwpersistence.DiffState(deltas.SegmentMatcher(), revert_radius=1)
print(state.update("Apples are red.", revision=1))
print(state.update("orange is ..", revision=2))
print(state.update("Apple is ..", revision=3))
print(state.update("Oranges is yellow", revision=4))
print(state.update("Oranges is yellow", revision=4))
