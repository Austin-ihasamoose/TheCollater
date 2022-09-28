import logging
import pandas as pd
import configparser
import os
import pathlib
import yaml
from time import strftime

""""
    Made this for a work project, needed to merge extensive databases into one for trending etc.
    Takes all input files in the input directory and collates them all into one target file. ALl files contain a 
    data sheet 'Data' so it is based off that.

    ex. 
        files/input/sheet1.xlsx
        files/input/sheet2.ods
        files/input/sheet1.json

        ->

        files/output/sheets_combined.xlsx
        
    Thanks to Ashi for helping me make this work multi-platform and doing a basic code review!

"""

XL_EXTENSIONS = ['xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt']
OTHER_EXTENSIONS = ['csv']
ALL_EXTENSIONS = ['csv', 'xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt']

IN_PATH = os.path.join('files', 'input')
OUT_PATH = os.path.join('files', 'output')


def check_formatting(files):
    """
    :param files: list of strings to determine formatting
    :return: File type or raise type error
    """

    extensions = {}
    extension_types = []

    for ftype in globals()['ALL_EXTENSIONS']:
        extensions.setdefault(ftype, 0)

    for file in files:
        raw_ext = pathlib.Path(file).suffix  # ex. '.xlsx'
        ext = raw_ext[1:]  # Remove the period
        if ext not in globals()['ALL_EXTENSIONS']:
            raise TypeError("Unsupported filetype in SRC directory. Acceptable filetypes: "
                            "'csv', 'json', 'xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt'")
        extensions[ext] += 1

    for key, value in extensions.items():
        if value >= 1:
            extension_types.append(key)

    return extension_types


def merge_dfs(files, filetype):
    """
    :param files: files as a list (string)
    :param filetype: Type of file being fed to merge (string)
    :return: Merged DF of all files in input
    """

    dfs = []
    df_out = pd.DataFrame()

    for file in files:
        path = os.path.join('files', 'input', file)
        if isinstance(filetype, list):  # If there is multiple files
            for ft in filetype:
                if ft in globals()['XL_EXTENSIONS']:
                    try:
                        full_df = pd.read_excel(path, sheet_name='Data', header=1, index_col=False)
                        # ^ Set second row as headings ^

                        for i in full_df.columns:  # Removes unwanted headers
                            if "Unnamed" in str(i):
                                full_df.drop(i, axis=1, inplace=True)

                        headers = pd.read_excel(path, sheet_name='Data', index_col=0, nrows=0).columns.tolist()
                        filtered_headers = [headers[4], headers[6]]
                        print(filtered_headers)
                        # ^ Valuable info under headers we missed in full_df as per request.

                        for i, col in enumerate(["Filename:", "Procedure:"]):

                            # Insert new column at end of columns with heading from list above.
                            # Only insert 1 value.
                            full_df.insert(len(full_df.columns), col, [filtered_headers[i]]+['']*(len(full_df.index)-1))

                        dfs.append(full_df)
                    except ValueError:
                        logging.error("'Data' sheet does not exist! No job completed.\n")
                        raise Exception("'Data' sheet does not exist!")
                if ft == 'csv':
                    full_df = pd.csv(path)
                    dfs.append(full_df)
                if ft == 'json':
                    full_df = pd.read_json(path)
                    dfs.append(full_df)
        else:
            if filetype in globals()['XL_EXTENSIONS']:
                try:
                    dfs.append(pd.read_excel(path, sheet_name='Data'))
                except ValueError:
                    logging.error("'Data' sheet does not exist! No job completed.\n")
                    raise Exception("'Data' sheet does not exist!")
            if filetype == 'csv':
                dfs.append(pd.read_csv(path))
            if filetype == 'json':
                dfs.append(pd.read_json(path))

    for i, df in enumerate(dfs):  # combines all data
        df_out = pd.concat([df_out, df])

    return df_out


def unique_name(path):
    """
    :param path: path including file
    :return: a unique name: (1), (2), etc after filename.
    """
    raw_ext = pathlib.Path(path).suffix  # ex. '.xlsx'
    filename = os.path.splitext(path)[0]
    counter = 1

    while os.path.exists(path):
        path = filename + " (" + str(counter) + ")" + raw_ext
        counter += 1

    return path


def output_handler(output_df, out_format):
    """
    :param output_df: Final output dataframe (pandas)
    :param out_format: Desired file format for output (xlsx, csv)
    :return: None
    """

    if str(out_format) not in globals()['ALL_EXTENSIONS']:
        raise TypeError("Illegal configuration file! Must be CSV/XLSX! Current value: " + str(out_format))
    if out_format == 'xlsx':
        uniq_name = unique_name(os.path.join(globals()['OUT_PATH'], 'output_' + strftime('%Y-%m-%d') + '.xlsx'))
        output_df.to_excel(uniq_name, index=False)
        logging.info("Files merged! File is now in the output folder: " + uniq_name)
        logging.info("")
    if out_format == 'csv':
        uniq_name = unique_name(os.path.join(globals()['OUT_PATH'], 'output_' + strftime('%Y-%m-%d') + '.csv'))
        output_df.to_csv(uniq_name, index=False)
        logging.info("Files merged! File is now in the output folder: " + uniq_name)
        logging.info("")

    return


def create_dfs():
    """
    :return: Pandas Dataframes as a list
    """
    try:
        if not os.path.exists(globals()['IN_PATH']):
            os.makedirs(globals()['IN_PATH'])
        if not os.path.exists(globals()['OUT_PATH']):
            os.mkdir(globals()['OUT_PATH'])
        files = os.listdir(globals()['IN_PATH'])
        if len(files) > 0:
            logging.info("Found files: " + str(files))
            format = check_formatting(files)
            logging.info("Format(s): " + str(format))
            df_out = merge_dfs(files, format)
            output_handler(df_out, config.get('OUTPUT_FILETYPE', 'Filetype'))

        else:
            logging.error("No files in input folder!")
            raise LookupError("No files in input folder!")

    except FileNotFoundError:
        logging.error("No eligible in input folder, nothing to merge!")
        raise Exception("No eligible in input folder, nothing to merge!\n"
                        "Accepted file types:\n"
                        "'csv', 'json', 'xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt'")


def read_yaml(file):
    with open(file, "r") as f:
        return yaml.safe_load(f)


def create_yaml():
    config.write(open(os.path.join('files', 'config.ini'), 'w'))


if __name__ == '__main__':

    config = configparser.ConfigParser(allow_no_value=True)
    startup = False  # This exists for first error handling

    if not os.path.exists(globals()['IN_PATH']):  # If initial directories don't exist, create them.
        startup = True
        os.makedirs(globals()['IN_PATH'])
        os.mkdir(globals()['OUT_PATH'])

    logfile = os.path.join("files", "Logger_{}.log".format(strftime('%Y-%m-%d')))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(message)s', datefmt='%Y-%m-%d, %H:%M:%S',
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()
        ]
    )

    if not os.path.exists(os.path.join('files', 'config.ini')):  # Create config file
        config['OUTPUT_FILETYPE'] = {'# xlsx or csv': None, 'Filetype': 'xlsx'}
        create_yaml()
    else:
        config.read(os.path.join('files', 'config.ini'))
        try:
            config.get('OUTPUT_FILETYPE', 'filetype')
        except configparser.NoOptionError:
            logging.info("Incorrectly configured config file. Regenerating...")
            config['OUTPUT_FILETYPE'] = {'# xlsx or csv': None, 'Filetype': 'xlsx'}
            create_yaml()

    if startup is True:
        logging.info("---First Startup Error---")
        logging.info("Initial folders don't exist, generating...")
        logging.info("Created directories and configuration file successfully. Please re-run after files have been"
                     "placed into files//input.\n")
    else:
        # If directories already exist let's actually run.
        create_dfs()
