#!/bin/sh

rsparql --results=csv --query=identifier-properties.sparql --service="https://query.wikidata.org/sparql" \
	| tail -n +2 \
	| grep 'http://www.wikidata.org/entity/P' \
	| sed -e 's|http://www.wikidata.org/entity/P||; s|\r$||' \
	| sort -n \
	| uniq \
	> identifier-properties


