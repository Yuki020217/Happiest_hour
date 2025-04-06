from mpi4py import MPI
from data_loader import process_ndjson_file

def merge_results(result, dict):
  for key, value in dict.items():
    if key in result:
      result[key] += value
    else:
      result[key] = value
