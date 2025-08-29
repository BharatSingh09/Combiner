from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
import pandas as pd
import os
import re
import csv
import uuid
from werkzeug.utils import secure_filename
from datetime import datetime
import tempfile
import shutil

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this to a random secret key
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create upload and output directories
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_file(file_path):
    """Load a CSV or XLSX file into a pandas DataFrame."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(file_path, header=None)
    elif ext in [".xls", ".xlsx"]:
        return pd.read_excel(file_path, header=None)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def normalize_signal_name(signal):
    """Normalize signal names by removing spaces and converting to uppercase for matching."""
    if pd.isna(signal) or signal == "" or signal == "-":
        return ""
    return re.sub(r'\s+', '', str(signal).upper().strip())

def build_toc_index(toc_df):
    """Build an index of TOC rows with their signal combinations."""
    toc_index = []
    data_rows = toc_df.iloc[2:]  # Skip header rows
    
    for idx, row in data_rows.iterrows():
        entry_signal = row.iloc[1] if pd.notna(row.iloc[1]) else ""
        exit_signal = row.iloc[2] if pd.notna(row.iloc[2]) else ""
        
        normalized_entry = normalize_signal_name(entry_signal)
        normalized_exit = normalize_signal_name(exit_signal)
        
        toc_index.append((idx, normalized_entry, normalized_exit, str(entry_signal), str(exit_signal)))
    
    return toc_index

def build_tp_groups(tp_df):
    """Build groups of TP rows by signal combination."""
    tp_groups = {}
    data_rows = tp_df.iloc[2:]  # Skip header rows
    
    for idx, row in data_rows.iterrows():
        entry_signal = row.iloc[0] if pd.notna(row.iloc[0]) else ""
        exit_signal = row.iloc[1] if pd.notna(row.iloc[1]) else ""
        
        normalized_entry = normalize_signal_name(entry_signal)
        normalized_exit = normalize_signal_name(exit_signal)
        
        # Skip empty combinations
        if normalized_entry == "" and normalized_exit == "":
            continue
            
        combo_key = (normalized_entry, normalized_exit)
        
        if combo_key not in tp_groups:
            tp_groups[combo_key] = []
        tp_groups[combo_key].append(idx)
    
    return tp_groups

def trim_toc_columns(toc_df, target_column="TLI Profile ID"):
    """Trim TOC dataframe to keep only columns up to and including the target column."""
    if len(toc_df) > 0:
        header_row = toc_df.iloc[0].astype(str)
        
        # Find the target column
        target_col_idx = None
        for idx, col_name in enumerate(header_row):
            if target_column.lower() in str(col_name).lower():
                target_col_idx = idx
                break
        
        if target_col_idx is not None:
            # Keep columns from 0 to target_col_idx (inclusive)
            toc_df = toc_df.iloc[:, :target_col_idx + 1]
        else:
            # Keep first 21 columns (indices 0-20)
            max_cols = min(21, len(toc_df.columns))
            toc_df = toc_df.iloc[:, :max_cols]
    
    return toc_df

def match_and_combine(toc_df, tp_df, output_file):
    """Combine TOC and TP files maintaining TOC structure exactly and matching TP rows."""
    # Trim TOC columns first
    toc_df = trim_toc_columns(toc_df)
    
    # Get dimensions
    toc_cols = len(toc_df.columns)
    tp_cols = len(tp_df.columns)
    
    # Build indices
    toc_index = build_toc_index(toc_df)
    tp_groups = build_tp_groups(tp_df)
    
    # Create result DataFrame starting with headers
    result_data = []
    
    # Add combined headers (first 2 rows) - will be replaced later
    for i in range(2):
        toc_row = toc_df.iloc[i].tolist()
        tp_row = tp_df.iloc[i].tolist()
        combined_row = toc_row + tp_row
        result_data.append(combined_row)
    
    # Track TP row usage to distribute evenly
    tp_usage = {combo: {"rows": tp_groups[combo][:], "used": 0} for combo in tp_groups}
    
    # Track matching and unmatching combos
    matched_combos = []
    unmatched_combos = []
    
    # Process each TOC row
    matches_found = 0
    for toc_row_idx, norm_entry, norm_exit, orig_entry, orig_exit in toc_index:
        # Get original TOC row data
        toc_row_data = toc_df.iloc[toc_row_idx].tolist()
        
        # Look for matching TP combination
        combo_key = (norm_entry, norm_exit)
        tp_row_data = ["-"] * tp_cols  # Default to dashes
        
        # Create combo display string (skip empty combos)
        combo_display = ""
        if orig_entry.strip() and orig_entry != "-":
            combo_display = orig_entry
        if orig_exit.strip() and orig_exit != "-":
            if combo_display:
                combo_display += " → " + orig_exit
            else:
                combo_display = orig_exit
        
        if combo_key in tp_usage and tp_usage[combo_key]["used"] < len(tp_usage[combo_key]["rows"]):
            # Get next available TP row for this combination
            tp_row_idx = tp_usage[combo_key]["rows"][tp_usage[combo_key]["used"]]
            tp_row_data = tp_df.iloc[tp_row_idx].tolist()
            tp_usage[combo_key]["used"] += 1
            matches_found += 1
            if combo_display and combo_display not in matched_combos:
                matched_combos.append(combo_display)
        else:
            # No match found
            if combo_display and combo_display not in unmatched_combos:
                unmatched_combos.append(combo_display)
        
        # Combine TOC and TP row data
        combined_row = toc_row_data + tp_row_data
        result_data.append(combined_row)
    
    # Find TP combos that weren't used (available in TP but not in TOC)
    unused_tp_combos = []
    for combo_key, usage_info in tp_usage.items():
        if usage_info["used"] == 0:  # No matches from TOC
            # Get original signal names from first TP row of this combo
            first_tp_idx = usage_info["rows"][0]
            tp_row = tp_df.iloc[first_tp_idx]
            entry_signal = str(tp_row.iloc[0]) if pd.notna(tp_row.iloc[0]) else ""
            exit_signal = str(tp_row.iloc[1]) if pd.notna(tp_row.iloc[1]) else ""
            
            combo_display = ""
            if entry_signal.strip() and entry_signal != "-":
                combo_display = entry_signal
            if exit_signal.strip() and exit_signal != "-":
                if combo_display:
                    combo_display += " → " + exit_signal
                else:
                    combo_display = exit_signal
            
            if combo_display:
                unused_tp_combos.append(combo_display)
    
    # Create final DataFrame
    final_df = pd.DataFrame(result_data)
    final_df.to_csv(output_file, index=False, header=False)
    
    return matches_found, len(toc_index), matched_combos, unmatched_combos, unused_tp_combos

def replace_headers(input_file, output_file):
    """Replace the first two rows with the specified headers."""
    # Define the new headers
    header_row_1 = [
        "S.No", "Entry Signal", "Exit Signal", "Section Type", "Line", "TSRMS Route Id", 
        "Signal Type", "Aspects of Entry Signal (Derived for Auto Signal)", 
        "Requires Aspect of Exit Signal", "Requires Points in Route", "-", 
        "Requires Track Circuit \"UP\" in Route", "TIN's(TrackIdentificationNumber)RequiresFree",
        "TIN'sRequiresFreeInOverlap", "CheckRFIDSequence", "-", 
        "DistanceBetweenEntry&ExitSignal(Meter)", "MovementAuthorityFromFootofEntrySignal(inSections)",
        "Authorized Speed (kmph) In OS", "Track Profile Id", "TLI Profile ID",
        "EntrySignal", "ExitSignal", "Profile Id", "Line", "Turnout Speed", "-", "-",
        "Permanent Speed Restriction", "-", "-", "-", "-", "-", "-", "-", "Gradient", "-", "-",
        "LC Gate", "-", "-", "-", "-", "-", "-", "Track Condition", "-", "-"
    ]
    
    header_row_2 = [
        "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "Normal", "Reverse", "-", "-", "-",
        "EntrySignalFootTag", "(LinkingDistance#En-RouteTag)", "-", "-", "-", "-",
        "-", "-", "-", "-.1", "Speed Value (kmph)", "Start Distance (m)", "Length (m)",
        "U Speed", "A Speed", "B Speed", "C Speed", "Start Distance (m).1", "Speed Length (m)",
        "Ref Tag ID", "Span", "Gradient Type (Downhill/ Uphill)", "Gradient Value", "Length (m).1",
        "LC ID Numeric/LC ID Alpha suffix", "LC Alpha ID", "LC Manning Type",
        "LC Class (Special, A, B, B1, C etc)", "LC Distance (m)", "LC Auto Whistling Enabled",
        "LC Auto Whistling Type", "Track Condition Type(Radio Hole/ Non stopping/Neutral section/Reversing area/Fouling Mark)",
        "Start Distance (m).2", "Length (m).2"
    ]
    
    # Read the existing file
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    # Replace first two rows
    if len(rows) >= 2:
        rows[0] = header_row_1
        rows[1] = header_row_2
    
    # Make sure all rows have the same number of columns
    max_cols = max(len(row) for row in rows) if rows else 0
    for row in rows:
        while len(row) < max_cols:
            row.append("-")
    
    # Write back to file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def clean_csv(input_file, output_file):
    """Clean the CSV by removing trailing whitespace while preserving '-' values."""
    cleaned_rows = []
    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            new_row = []
            for cell in row:
                if str(cell).strip() == "-":
                    new_row.append("-")
                else:
                    new_row.append(str(cell).rstrip(" \t\n"))
            cleaned_rows.append(new_row)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(cleaned_rows)

def update_first_column_to_float(input_file, output_file):
    """Update first column values to float format (add .0 to integers)."""
    updated_rows = []
    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row_idx, row in enumerate(reader):
            if row:
                first_val = str(row[0]).strip()
                # Skip header rows (first 2 rows) and check if it's a pure integer
                if row_idx >= 2 and first_val.lstrip("-").isdigit():
                    row[0] = str(float(first_val))
            updated_rows.append(row)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(updated_rows)

def process_files(toc_file_path, tp_file_path, output_filename):
    """Complete processing pipeline."""
    try:
        # Load files
        toc_df = load_file(toc_file_path)
        tp_df = load_file(tp_file_path)
        
        # Generate unique temporary file names
        temp_id = str(uuid.uuid4())[:8]
        temp_combined = os.path.join(OUTPUT_FOLDER, f"temp_combined_{temp_id}.csv")
        temp_headers = os.path.join(OUTPUT_FOLDER, f"temp_headers_{temp_id}.csv")
        temp_cleaned = os.path.join(OUTPUT_FOLDER, f"temp_cleaned_{temp_id}.csv")
        final_output = os.path.join(OUTPUT_FOLDER, output_filename)
        
        # Process files
        matches_found, total_toc_rows, matched_combos, unmatched_combos, unused_tp_combos = match_and_combine(toc_df, tp_df, temp_combined)
        replace_headers(temp_combined, temp_headers)
        clean_csv(temp_headers, temp_cleaned)
        update_first_column_to_float(temp_cleaned, final_output)
        
        # Cleanup temporary files
        for temp_file in [temp_combined, temp_headers, temp_cleaned]:
            try:
                os.remove(temp_file)
            except FileNotFoundError:
                pass
        
        return {
            'success': True,
            'output_file': final_output,
            'matches_found': matches_found,
            'total_toc_rows': total_toc_rows,
            'filename': output_filename,
            'matched_combos': matched_combos,
            'unmatched_combos': unmatched_combos,
            'unused_tp_combos': unused_tp_combos
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    # Check if files are present
    if 'toc_file' not in request.files or 'tp_file' not in request.files:
        flash('Both TOC and TP files are required!', 'error')
        return redirect(url_for('index'))
    
    toc_file = request.files['toc_file']
    tp_file = request.files['tp_file']
    output_filename = request.form.get('output_filename', 'final_combined.csv')
    
    # Validate files
    if toc_file.filename == '' or tp_file.filename == '':
        flash('Please select both files!', 'error')
        return redirect(url_for('index'))
    
    if not (allowed_file(toc_file.filename) and allowed_file(tp_file.filename)):
        flash('Only CSV, XLS, and XLSX files are allowed!', 'error')
        return redirect(url_for('index'))
    
    # Ensure output filename has .csv extension
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'
    
    try:
        # Save uploaded files
        toc_filename = secure_filename(toc_file.filename)
        tp_filename = secure_filename(tp_file.filename)
        
        toc_path = os.path.join(UPLOAD_FOLDER, toc_filename)
        tp_path = os.path.join(UPLOAD_FOLDER, tp_filename)
        
        toc_file.save(toc_path)
        tp_file.save(tp_path)
        
        # Process files
        result = process_files(toc_path, tp_path, output_filename)
        
        # Cleanup uploaded files
        os.remove(toc_path)
        os.remove(tp_path)
        
        if result['success']:
            flash(f'Files processed successfully! Matched {result["matches_found"]}/{result["total_toc_rows"]} TOC rows.', 'success')
            return render_template('result.html', 
                                 filename=result['filename'],
                                 matches_found=result['matches_found'],
                                 total_toc_rows=result['total_toc_rows'],
                                 matched_combos=result['matched_combos'],
                                 unmatched_combos=result['unmatched_combos'],
                                 unused_tp_combos=result['unused_tp_combos'])
        else:
            flash(f'Error processing files: {result["error"]}', 'error')
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    file_path = os.path.join(OUTPUT_FOLDER, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash('File not found!', 'error')
        return redirect(url_for('index'))

@app.route('/api/process', methods=['POST'])
def api_process():
    """API endpoint for processing files programmatically."""
    try:
        if 'toc_file' not in request.files or 'tp_file' not in request.files:
            return jsonify({'error': 'Both TOC and TP files are required'}), 400
        
        toc_file = request.files['toc_file']
        tp_file = request.files['tp_file']
        
        if not (allowed_file(toc_file.filename) and allowed_file(tp_file.filename)):
            return jsonify({'error': 'Only CSV, XLS, and XLSX files are allowed'}), 400
        
        # Save files temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as toc_temp:
            toc_file.save(toc_temp.name)
            toc_path = toc_temp.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tp_temp:
            tp_file.save(tp_temp.name)
            tp_path = tp_temp.name
        
        # Process files
        output_filename = f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        result = process_files(toc_path, tp_path, output_filename)
        
        # Cleanup temporary files
        os.unlink(toc_path)
        os.unlink(tp_path)
        
        if result['success']:
            return jsonify({
                'success': True,
                'download_url': f'/download/{result["filename"]}',
                'matches_found': result['matches_found'],
                'total_toc_rows': result['total_toc_rows'],
                'matched_combos': result['matched_combos'],
                'unmatched_combos': result['unmatched_combos'],
                'unused_tp_combos': result['unused_tp_combos']
            })
        else:
            return jsonify({'error': result['error']}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)