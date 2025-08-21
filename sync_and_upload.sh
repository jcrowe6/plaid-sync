#!/bin/bash
PATH=/home/jcrowell/.local/bin:$PATH
source .venv/bin/activate
uv run plaid-sync.py -c config/prod.ini --cursor-sync
uv run upload.py
