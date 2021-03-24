#!/bin/bash
while true
do
python3 create_liquidiations_pickle_dump.py
cp liquidations liquidations.backup
sleep 800
done
