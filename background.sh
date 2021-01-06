#!/bin/bash
target/release/wikidata-filter $@ > log 2>&1 &
disown
