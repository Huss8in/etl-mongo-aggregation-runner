# ETL Mongo Aggregation Runner

This script runs MongoDB aggregation pipelines and exports results to CSV files.

## Features
- Loads and executes aggregation pipelines from JSON files.
- Supports optional date filtering.
- Saves results in the `CSVs` folder.
- Uses `tqdm` for progress tracking.

## Usage
1. **Activate the virtual environment** (if not already activated):
   - On Windows (PowerShell):
```sh
     .\venv\Scripts\Activate
```
   - On macOS/Linux:
 ```sh
     source venv/bin/activate
 ```

2. **Install dependencies**:
```sh
   pip install -r requirements.txt
```

3. **Run the script**:
With a date range:
   ```sh
   python script.py "01/01/2024" "31/01/2024"
```
Without dates (fetches all data):
   ```sh
   python script.py
```