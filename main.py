from mpi4py import MPI
from data_loader import process_ndjson_file
import time
import sys


def merge_results(result, dict):
    for key, value in dict.items():
        if key in result:
            result[key] += value
        else:
            result[key] = value
    return result


def print_top_k_hours(hour_scores, k=5, top=True):
    sorted_result = sorted(hour_scores.items(), key=lambda x: x[1], reverse=top)
    for i, (hour, score) in enumerate(sorted_result[:k], 1):
        print(f"({i}) {hour} with an overall sentiment score of {score}")


def print_top_k_users(user_scores, k=5, top=True):
    sorted_result = sorted(user_scores.items(), key=lambda x: x[1], reverse=top)
    for i, ((username,account_id), score) in enumerate(sorted_result[:k], 1):
        print(f"({i}) {username}, account id {account_id} with an overall sentiment score of {score}")


def main():
    start_time = time.time()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    filepath = sys.argv[1]
    hour_scores, user_scores = process_ndjson_file(filepath, rank, size)
    result = comm.gather((hour_scores, user_scores), root=0)

    if rank == 0:
        final_hour_scores = {}
        final_user_scores = {}

        for hour_part, user_part in result:
            final_hour_scores = merge_results(final_hour_scores, hour_part)
            final_user_scores = merge_results(final_user_scores, user_part)

        end_time = time.time()
        duration = end_time - start_time
        print("Execution Time::", duration)

        print("the 5 happiest hours")
        print_top_k_hours(final_hour_scores, top=True)
        print("the 5 saddest hours")
        print_top_k_hours(final_hour_scores, top=False)
        print("the 5 happiest people")
        print_top_k_users(final_user_scores, top=True)
        print("the 5 saddest people")
        print_top_k_users(final_user_scores, top=False)


if __name__ == "__main__":
    main()







