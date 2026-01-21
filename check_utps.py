#!/usr/bin/env python3
import json

with open('data/initialization.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for m in data['municipios']:
    if m['cd_mun'] in [1506203, 1507474]:
        print(f"{m['nm_mun']} ({m['cd_mun']}): UTP {m['utp_id']}")
