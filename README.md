# Ytel Carrier Lookup

High-performance batch carrier lookup tool using the Ytel API.

## Features
- High throughput (batching + connection pooling)
- Automatic auth via .env.ytel
- Auto token refresh
- Resume-safe runs with timestamped folders
- Pass-through of all input CSV fields
- Master results + Verizon-only results
- Live progress and stats

## Usage

### New run
python ytel_carrierlookup.py

### Resume run
python ytel_carrierlookup.py --resume <run_folder>

## Input
Input CSV must include a `phone_number` column. All other fields are passed through.

## Auth
Create a `.env.ytel` file:

username=YOUR_USERNAME
password=YOUR_PASSWORD

Do not commit this file.
