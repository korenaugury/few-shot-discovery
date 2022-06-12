
import os
import pickle
import json

import yaml
import gzip
import shutil
import zipfile


def is_file_exist(file_path):
    return os.path.isfile(file_path)


def is_folder_exist(folder_path):
    return os.path.isdir(folder_path)


def get_current_working_directory():
    return os.getcwd()


def get_file_name_from_file_path(file_path):
    return os.path.basename(file_path)


def get_folder_path_from_file_path(file_path):
    return os.path.dirname(file_path)


def delete_folder(folder_path):
    shutil.rmtree(folder_path)


def delete_file(file_path):
    os.remove(file_path)


def rename_file(full_file_path, new_file_name):
    new_full_path = os.path.join(os.path.split(full_file_path)[0], new_file_name)
    os.rename(full_file_path, new_full_path)


def make_folder(folder_path):
    os.makedirs(folder_path, exist_ok=True)


def make_folder_for_file_creation_if_not_exists(file_path):
    folder_path = os.path.dirname(file_path)
    if folder_path:
        make_folder(folder_path)


def save_to_json(data, file_path):
    make_folder_for_file_creation_if_not_exists(file_path)
    with open(file_path, 'w') as outfile:
        json.dump(data, outfile)
        outfile.close()


def save_to_pickle(obj, file_path):
    make_folder_for_file_creation_if_not_exists(file_path)
    with open(file_path, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
        handle.close()


def write_list_of_elements_to_text_file(lines, file_path):
    make_folder_for_file_creation_if_not_exists(file_path)
    text_file = open(file_path, "w")
    for line in lines:
        text_file.write(str(line) + "\n")

    text_file.close()


def load_json(file_path):
    with open(file_path) as f:
        json_file = json.load(f)
        f.close()
    return json_file


def load_multiline_json(file_path):
    entries = []
    with open(file_path) as f:
        for line in f:
            entries.append(json.loads(line))
        f.close()

    return entries


def load_pickle(file_path):
    with open(file_path, 'rb') as handle:
        pkl_file = pickle.load(handle)
        handle.close()
    return pkl_file


def load_yaml(file_path):
    with open(file_path, 'r') as stream:
        return yaml.safe_load(stream)


def read_text_file_lines(file_path):
    file = open(file_path, 'r')
    lines = file.readlines()
    file.close()

    return lines


def compress_file(file_path):
    make_folder_for_file_creation_if_not_exists(file_path)
    with open(file_path, 'rb') as f_in, gzip.open(file_path +'.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def compress_files_to_zip_archive(file_paths_to_compress, zip_path):

    zip_obj = zipfile.ZipFile(zip_path, 'w')
    for file_path in file_paths_to_compress:
        zip_obj.write(file_path, compress_type=zipfile.ZIP_DEFLATED)

    zip_obj.close()
