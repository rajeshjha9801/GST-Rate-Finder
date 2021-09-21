import os
import pickle
def load_pickle_file(filename, base_path=None):
    if not base_path:
        base_path = os.path.join(os.getcwd(),'pickles')
    file_path=os.path.join(base_path, filename)
    assert os.path.exists(file_path) and os.path.isfile(file_path), "Pickle file does not exist :: "+file_path
    return pickle.load(open(file_path,'rb'))

def write_pickle_file(obj, filename, base_path=None):
    if not base_path:
        base_path = os.path.join(os.getcwd(),'pickles')
    assert os.path.exists(base_path) and os.path.isdir(base_path), "Base dir does not exist :: "+base_path
    file_path=os.path.join(base_path, filename)    
    pickle.dump(obj, open(file_path,'wb'))
    return True
    