#!/bin/bash
spark-submit programaestudiante.py persona*.json
python -m pytest -vv
