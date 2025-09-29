# -*- coding: utf-8 -*-
"""Flask web app for processing and enriching company names."""

import os
import threading
import time
import glob
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
import pandas as pd
import normalize_companies as norm
import enrich_websites as enrich

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this'

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

progress_data = {
    'normalize': {'status': 'idle', 'progress': 0, 'message': 'Ready to start'},
    'enrich': {'status': 'idle', 'progress': 0, 'message': 'Ready to start'},
    'current_file': None
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_file_stats():
    stats = {
        'normalized_file': None,
        'batch_count': 0,
        'enriched_files': [],
        'total_representatives': 0
    }
    
    normalized_file = os.path.join(norm.output_directory, 'minimal_normalized.csv')
    if os.path.exists(normalized_file):
        try:
            df = pd.read_csv(normalized_file)
            stats['normalized_file'] = {
                'path': normalized_file,
                'rows': len(df),
                'size': os.path.getsize(normalized_file)
            }
        except Exception:
            pass
    
    if os.path.exists(norm.batch_directory):
        batch_files = glob.glob(os.path.join(norm.batch_directory, 'batch_*.csv'))
        stats['batch_count'] = len(batch_files)
        try:
            stats['total_representatives'] = sum(len(pd.read_csv(f)) for f in batch_files)
        except Exception:
            stats['total_representatives'] = 0
    
    if os.path.exists(enrich.OUTPUT_DIR):
        enriched_files = glob.glob(os.path.join(enrich.OUTPUT_DIR, '*_enriched.csv'))
        stats['enriched_files'] = []
        for f in enriched_files:
            try:
                df = pd.read_csv(f)
                stats['enriched_files'].append({
                    'name': os.path.basename(f),
                    'rows': len(df),
                    'size': os.path.getsize(f)
                })
            except Exception:
                continue
    
    return stats

def run_normalization():
    progress_data['normalize']['status'] = 'running'
    progress_data['normalize']['progress'] = 0
    progress_data['normalize']['message'] = 'Starting normalization...'
    
    try:
        for i in range(5):
            time.sleep(1)
            progress_data['normalize']['progress'] = (i + 1) * 20
            progress_data['normalize']['message'] = f'Processing step {i + 1}/5...'
        
        norm.main()
        
        progress_data['normalize']['status'] = 'completed'
        progress_data['normalize']['progress'] = 100
        progress_data['normalize']['message'] = 'Normalization completed successfully!'
        
    except Exception as e:
        progress_data['normalize']['status'] = 'error'
        progress_data['normalize']['message'] = f'Error: {str(e)}'

def run_enrichment():
    progress_data['enrich']['status'] = 'running'
    progress_data['enrich']['progress'] = 0
    progress_data['enrich']['message'] = 'Starting website enrichment...'
    
    try:
        # Progress callback from enrichment
        def on_progress(current, total, message):
            try:
                if total and total > 0:
                    pct = int((current / total) * 100)
                    # Clamp 0-99 until completion
                    progress_data['enrich']['progress'] = max(0, min(99, pct))
                else:
                    # Unknown total: show indeterminate style by small increments
                    progress_data['enrich']['progress'] = min(95, progress_data['enrich']['progress'] + 1)
                progress_data['enrich']['message'] = message
            except Exception:
                pass

        # Use progress-enabled runner if available
        if hasattr(enrich, 'run_with_progress'):
            enrich.run_with_progress(on_progress)
        else:
            enrich.main()
        
        progress_data['enrich']['status'] = 'completed'
        progress_data['enrich']['progress'] = 100
        progress_data['enrich']['message'] = 'Enrichment completed successfully!'
        
    except Exception as e:
        progress_data['enrich']['status'] = 'error'
        progress_data['enrich']['message'] = f'Error: {str(e)}'

@app.route('/')
def index():
    stats = get_file_stats()
    return render_template('index.html', stats=stats, progress=progress_data)

@app.route('/results')
def results():
    stats = get_file_stats()
    preview_rows = 50
    
    normalized_path = os.path.join(norm.output_directory, 'minimal_normalized.csv')
    normalized_df = None
    if os.path.exists(normalized_path):
        try:
            normalized_df = pd.read_csv(normalized_path, nrows=preview_rows)
        except Exception:
            normalized_df = None

    enriched_files = []
    if os.path.exists(enrich.OUTPUT_DIR):
        enriched_files = glob.glob(os.path.join(enrich.OUTPUT_DIR, '*_enriched.csv'))
        enriched_files.sort()

    enriched_preview = []
    for fpath in enriched_files[:5]:
        try:
            df = pd.read_csv(fpath, nrows=preview_rows)
            enriched_preview.append({
                'name': os.path.basename(fpath),
                'rows': len(pd.read_csv(fpath)),
                'size': os.path.getsize(fpath),
                'head': df.to_dict(orient='records')
            })
        except Exception:
            continue

    return render_template('results.html',
                           stats=stats,
                           normalized_head=(normalized_df.to_dict(orient='records') if normalized_df is not None else None),
                           enriched_preview=enriched_preview)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file selected')
        return redirect(url_for('index'))
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected')
        return redirect(url_for('index'))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        norm.input_path = filepath
        progress_data['current_file'] = filename
        
        flash(f'File {filename} uploaded successfully!')
    else:
        flash('Invalid file type. Please upload a CSV file.')
    
    return redirect(url_for('index'))

@app.route('/start_normalize', methods=['POST'])
def start_normalize():
    if progress_data['normalize']['status'] == 'running':
        return jsonify({'error': 'Normalization already in progress'})
    
    thread = threading.Thread(target=run_normalization)
    thread.start()
    
    return jsonify({'message': 'Normalization started'})

@app.route('/start_enrich', methods=['POST'])
def start_enrich():
    if progress_data['enrich']['status'] == 'running':
        return jsonify({'error': 'Enrichment already in progress'})
    
    thread = threading.Thread(target=run_enrichment)
    thread.start()
    
    return jsonify({'message': 'Enrichment started'})

@app.route('/progress')
def get_progress():
    stats = get_file_stats()
    return jsonify({
        'progress': progress_data,
        'stats': stats
    })

@app.route('/download/<file_type>')
def download_file(file_type):
    if file_type == 'normalized':
        file_path = os.path.join(norm.output_directory, 'minimal_normalized.csv')
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name='normalized_companies.csv')
    elif file_type == 'enriched':
        enriched_files = glob.glob(os.path.join(enrich.OUTPUT_DIR, '*_enriched.csv'))
        if enriched_files:
            return send_file(enriched_files[0], as_attachment=True)
    
    flash('File not found')
    return redirect(url_for('index'))

@app.route('/reset')
def reset_progress():
    progress_data['normalize'] = {'status': 'idle', 'progress': 0, 'message': 'Ready to start'}
    progress_data['enrich'] = {'status': 'idle', 'progress': 0, 'message': 'Ready to start'}
    return redirect(url_for('index'))

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(norm.output_directory, exist_ok=True)
    os.makedirs(enrich.OUTPUT_DIR, exist_ok=True)
    app.run(debug=True, host='0.0.0.0', port=5000)