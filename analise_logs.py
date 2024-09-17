import threading

def split_log_file(filename, num_parts):
    with open(filename, 'r') as file:
        lines = file.readlines()

    part_size = len(lines) // num_parts
    file_parts = []

    for i in range(num_parts):
        start = i * part_size
        end = (i + 1) * part_size if i != num_parts - 1 else len(lines)
        file_parts.append(lines[start:end])

    return file_parts


def process_log_part(log_part, thread_id, results):
    accesses_per_hour = {}
    successful_responses = 0

    for line in log_part:
        try:
            parts = line.split(' ')
            timestamp = parts[3][1:]
            status_code = parts[8]

            hour = timestamp.split(':')[1] + ":00"

            if hour in accesses_per_hour:
                accesses_per_hour[hour] += 1
            else:
                accesses_per_hour[hour] = 1

            if status_code == '200':
                successful_responses += 1
        except IndexError:
            continue

    results[thread_id] = (accesses_per_hour, successful_responses)
    print(f"Thread {thread_id} finalizada")

def main():
    log_filename = 'access.log'

    num_threads = 4
    log_parts = split_log_file(log_filename, num_threads)

    threads = []
    results = [None] * num_threads
    for i in range(num_threads):
        thread = threading.Thread(target=process_log_part, args=(log_parts[i], i, results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    combined_accesses_per_hour = {}
    total_successful_responses = 0

    for accesses_per_hour, successful_responses in results:
        for hour, count in accesses_per_hour.items():
            if hour in combined_accesses_per_hour:
                combined_accesses_per_hour[hour] += count
            else:
                combined_accesses_per_hour[hour] = count

        total_successful_responses += successful_responses

    print(f"")

    for hour, count in sorted(combined_accesses_per_hour.items()):
        print(f"{hour}: {count} acessos")

    print(f"\nCódigo 200: {total_successful_responses}")


if __name__ == "__main__":
    main()
