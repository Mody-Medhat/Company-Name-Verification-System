# Company Name Verification System

A comprehensive web application for processing and enriching company name data with website verification.

## Demo

```bash
ffmpeg -i "Demo/CNVS Demo.mkv" -c:v libx264 -crf 23 -preset veryfast -c:a aac -b:a 128k Demo/demo.mp4
```

Then link it in the README (GitHub will show a player on the file page):

- [Watch the demo (MP4)](Demo/demo.mp4)

## Features

- **Web Interface**: User-friendly dashboard with real-time progress tracking
- **File Upload**: Easy CSV file upload and processing
- **Data Normalization**: Clean, normalize, and cluster company names
- **Website Enrichment**: Find and verify official company websites
- **Results Viewer**: Preview and download processed data
- **Real-time Updates**: Live progress bars and statistics

## Quick Start

### 1) Install Dependencies
```bash
pip install -r requirements.txt
```

### 2) Start the Web Application
```bash
python run_webapp.py
```

### 3) Open Your Browser
Go to: http://localhost:5000

## Usage

1. **Upload Data**: Upload a CSV file with company names
2. **Normalize**: Click "Start Normalization" to clean and cluster names
3. **Enrich**: Click "Start Enrichment" to find company websites
4. **View Results**: Check the Results page to preview data
5. **Download**: Download processed files when ready

## File Structure

```
├── app.py                    # Main Flask application
├── run_webapp.py            # Web app launcher
├── normalize_companies.py   # Data normalization module
├── enrich_websites.py       # Website enrichment module
├── requirements.txt         # Python dependencies
├── templates/               # HTML templates
│   ├── base.html           # Base template
│   ├── index.html          # Dashboard page
│   └── results.html        # Results viewer
└── README.md               # This file
```

## Command Line Interface (Alternative)

If you prefer command line processing:

```bash
# Normalize company names
python normalize_companies.py

# Enrich with website data
python enrich_websites.py
```

## Configuration

Key settings can be modified in the respective modules:

- **Normalization**: `normalize_companies.py` - thresholds, batch sizes
- **Enrichment**: `enrich_websites.py` - search settings, confidence thresholds
- **Web App**: `app.py` - upload limits, file paths

## Output Files

- `enrichment_artifacts/minimal_normalized.csv` - Normalized company data
- `enrichment_artifacts/batches/batch_*.csv` - Processing batches
- `enrichment_results/*_enriched.csv` - Website-enriched data

## Requirements

- Python 3.7+
- See `requirements.txt` for full dependency list
