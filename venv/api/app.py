from flask import Flask, render_template, request, send_file
import os
import pandas as pd
import tempfile

app = Flask(__name__)

def merge_csv_files(csv_files, output_file):
    if not csv_files:
        return None

    # Read the first CSV file to initialize the merged dataframe
    merged_df = pd.read_csv(csv_files[0])

    # Merge subsequent CSV files with outer join on common columns if found
    for file in csv_files[1:]:
        current_df = pd.read_csv(file)

        # Find common columns
        common_columns = set(merged_df.columns) & set(current_df.columns)

        if common_columns:
            # Merge only if common columns are found
            merged_df = pd.merge(merged_df, current_df, how='outer', on=list(common_columns))

    # Save the merged dataframe to a CSV file
    merged_df.to_csv(output_file, index=False)

    return output_file

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/merge_csv', methods=['POST'])
def merge_csv():
    uploaded_files = request.files.getlist('files[]')

    if not uploaded_files or not all(file.filename for file in uploaded_files):
        return "Please select at least one CSV file to upload."

    temp_dir = tempfile.mkdtemp()
    csv_files = []

    for file in uploaded_files:
        if file and file.filename.endswith('.csv'):
            file_path = os.path.join(temp_dir, file.filename)
            file.save(file_path)
            csv_files.append(file_path)

    output_file = os.path.join(temp_dir, 'merged_output.csv')
    merged_file = merge_csv_files(csv_files, output_file)

    if merged_file:
        return send_file(merged_file, as_attachment=True, download_name='merged_output.csv')
    else:
        return "Error occurred during CSV merging."

if __name__ == "__main__":
    app.run(debug=True)
